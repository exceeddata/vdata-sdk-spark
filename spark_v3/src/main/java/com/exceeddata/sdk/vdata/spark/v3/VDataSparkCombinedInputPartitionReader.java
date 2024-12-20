/*
 * Copyright (C) 2023-2025 Smart Software for Car Technologies Inc. and EXCEEDDATA
 *     https://www.smartsct.com
 *     https://www.exceeddata.com
 *
 *                            MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * Except as contained in this notice, the name of a copyright holder
 * shall not be used in advertising or otherwise to promote the sale, use
 * or other dealings in this Software without prior written authorization
 * of the copyright holder.
 */

package com.exceeddata.sdk.vdata.spark.v3;

import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConstants.COLUMN_DEVICE;
import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConstants.COLUMN_TIME;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exceeddata.ac.common.data.record.Record;
import com.exceeddata.ac.common.data.typedata.InstantData;
import com.exceeddata.ac.common.data.typedata.IntData;
import com.exceeddata.ac.common.message.MessageContent;
import com.exceeddata.ac.common.message.MessageDecoder;
import com.exceeddata.ac.common.message.MessageEncoder;
import com.exceeddata.ac.format.util.MessageDecodeBuilder;
import com.exceeddata.ac.format.util.MessageEncodeBuilder;
import com.exceeddata.sdk.vdata.binary.BinarySeekableReader;
import com.exceeddata.sdk.vdata.data.VDataFrame;
import com.exceeddata.sdk.vdata.data.VDataReader;
import com.exceeddata.sdk.vdata.data.VDataReaderFactory;
import com.exceeddata.sdk.vdata.data.VDataRow;
import com.exceeddata.sdk.vdata.spark.v3.message.SignalNameFormatter;
import com.exceeddata.sdk.vdata.spark.v3.message.VswMessage;
import com.exceeddata.sdk.vdata.spark.v3.message.VswSparkRawDataRow;
import com.exceeddata.sdk.vdata.spark.v3.message.VswSparkRow;
import com.exceeddata.sdk.vdata.spark.v3.reader.LittleEndianSeekableFSReader;


/**
 * 替换VDataSparkInputPartitionReader
 */
public class VDataSparkCombinedInputPartitionReader implements PartitionReader<InternalRow> {

    private final static Logger logger = LoggerFactory.getLogger(VDataSparkCombinedInputPartitionReader.class);

    private List<String> filePath = null;
    private int densifyRowsAhead = 0;
    private List<String> targetColumns = null;
    private FSDataInputStream istream = null;
    private Filter[] filters = null;
    protected VDataReader vdataReader = null;
    protected VDataFrame vdataFrame = null;
    protected List<String> vdataColumns = null;
    protected Iterator<VDataRow> vrowIter = null;
    protected String deviceid = null;
    private int [] columnSources=null;
    private boolean applyFormula;
    private MessageDecoder dbcDecoder =null;
    private MessageEncoder dbcEncoder = null;
    private List <String> vsw_cols = new ArrayList<>();
    private List <MessageContent > col2msgInfo = new ArrayList<>();
    private Object [][] currentVswData =null ;
    private int currentVswIndex=0;
    private Object [] currentRow;
    private Record currentRecord;
    private int file_index =0;
    private int samplingFrequency;

    Iterator<Map.Entry<String,VDataReader >> readers;


    public VDataSparkCombinedInputPartitionReader(final VDataSparkCombinedInputPartition inputPartition) {
        this(inputPartition.getFilePath(),
                inputPartition.getSchema(),
                inputPartition.getFilters(),
                inputPartition.getDensifyRowsAhead(),
                inputPartition.getDbcPaths(),
                inputPartition.getApplyFormula(),
                inputPartition.getSignalNameFormatter(),
                inputPartition.getSamplingFrequency());

    }

    public VDataSparkCombinedInputPartitionReader(final List<String> filePath,
                                                  final StructType schema,
                                                  final Filter[] filters,
                                                  final int densifyRowsAhead,
                                                  final String dbcPaths,
                                                  final boolean applyFormula,
                                                  final SignalNameFormatter signalNameFormatter,
                                                  final int samplingFrequency) {
        this.filePath = filePath;
        this.filters = filters;
        this.densifyRowsAhead = densifyRowsAhead > 0 ? densifyRowsAhead : 0;
        this.samplingFrequency = samplingFrequency;

        if (schema != null && schema.size() > 0) {
            targetColumns = Arrays.asList(schema.fieldNames());
        }
        if (targetColumns == null || targetColumns.size() == 0) {
            throw new RuntimeException("schema not provided");
        }

        columnSources= VDataSparkUtils.getColumnSources(targetColumns);
        this.applyFormula = applyFormula;

        //Try to load DBC
        if (dbcPaths != null ) {
            try {
                dbcDecoder = MessageDecodeBuilder.buildDBC(dbcPaths, false, true, true, false, false, null);
                dbcEncoder = MessageEncodeBuilder.buildDBC(dbcPaths, true, true, false);
            } catch (Exception e) {
                logger.error("dbc analyze error: " + e);
            }
            //Map User Select column to VSW Column name via dbc
            //mock a RecordMessage with selected columns
            //check output for channelId/MessageId
            //encode the channelId/MessageId to VSW column name
            //store the mapping from column idx to channelId/MessageId for output data

            //TODO Check the format constrains for column name. e.g. full qualified name must be MessageName.AttributeName
            //may conflict with other rules. e.g.  DatabaseName.columnName...
            //so some other conversion may required here
            Record mockRecord = new Record();
            for (String col : targetColumns) {
                mockRecord.add(col, new IntData(0));
            }
            ArrayList<MessageContent> msgs = dbcEncoder.encode(mockRecord);

            vsw_cols = new ArrayList<>();
            for (MessageContent msg : msgs) {
                //TODO add customer interface for mapping the channel_id, message id to vsw column name
//                vsw_cols.add(String.format( "0x%02x_%02x",msg.getChannelID(), msg.getMessageID() ));
//                vsw_cols.add(String.format("%d_%d", msg.getChannelID(), msg.getMessageID()));
                vsw_cols.add(signalNameFormatter.getSignalName(msg.getChannelID(), msg.getMessageID()));
                col2msgInfo.add(msg);
            }
        }

        loadFile();
    }

    @Override
    public void close() throws IOException {
        safeCloseResources();
    }

    @Override
    public boolean next() throws IOException {
        if (currentVswData !=null && currentVswIndex< currentVswData.length ){
            currentRow = currentVswData[currentVswIndex ++];
            if (dbcDecoder != null ){
                currentRecord = decodeRow(currentRow);
            }
            return true;
        }
        //try to load data from next file
        if (loadFile()){
            currentRow = currentVswData[currentVswIndex ++];
            if (dbcDecoder != null ){
                currentRecord = decodeRow(currentRow);
            }
            return true;
        }
        return false;
    }

    @Override
    public InternalRow get() {
        if (null != dbcDecoder){
            return  new VswSparkRawDataRow(deviceid, currentRecord, targetColumns ,columnSources);
        }else {
            return new VswSparkRow(deviceid, currentRow, targetColumns, columnSources);
        }
    }

    public List<String> getTargetColumns() {
        return targetColumns;
    }

    public void setTargetColumns(final List<String> targetColumns) {
        this.targetColumns = targetColumns;
    }



    @SuppressWarnings("deprecation")
    protected synchronized boolean loadFile() {
        try {
            /*
             * file name should be device_rollout_datetime.extension format
             *
             * also we could store device in extended info array at customization
             */
            while (true) {

                if(null != readers){
                    //Multi VSW Reader mode
                    //dirty code, to be moved to standalone function
                    while(readers.hasNext()) {
                        Map.Entry<String, VDataReader> entry = readers.next();
                        deviceid = entry.getKey();
                        boolean device_not_match= false;
                        for (final Filter filter : filters) {
                            if (COLUMN_DEVICE.equals(filter.references()[0])) {
                                if (!VDataSparkFilters.filter(filter, deviceid)) {
                                    device_not_match = true;
                                    break;
                                }
                            }
                        }

                        if (device_not_match){
                            continue;
                        }
                        vdataReader = entry.getValue();
                        vdataFrame = vdataReader.df();
                        vdataColumns = vdataFrame.cols();

                        if(samplingFrequency == 0){
                            currentVswData = vdataFrame.objects(densifyRowsAhead);
                        }else{
                            currentVswData = vdataFrame.sampling(samplingFrequency);
                        }
                        currentVswIndex =0;
                        return true;
                    }
                    //no suitable VSW found. check next file.
                    readers= null;
                }

                safeCloseResources();
                if (file_index >= filePath.size()) {
                    return false;
                }
                final Path path = new Path(filePath.get(file_index++));
                final String pathname = path.getName().trim();
                final int extindex = pathname.lastIndexOf('.');
                final String filename = extindex > 0 ? pathname.substring(0, extindex) : pathname;
                if (pathname.endsWith("mvsw")){
                    istream = path.getFileSystem(new Configuration()).open(path);
                    ArrayList<BinarySeekableReader> dr= new ArrayList<>();
                    dr.add(new LittleEndianSeekableFSReader(istream));
                    VDataReaderFactory f = new VDataReaderFactory();
                    f.setDataReaders(dr);
                    if (this.dbcEncoder  == null ){
                        f.setSignals(targetColumns);
                    }else {
                        //DBC Mode...
                        f.setSignals(vsw_cols);
                    }
                    readers = f.openMultipleVswFormats();
                    //dirty code, to be moved to standalone function
                    while(readers.hasNext()) {
                        Map.Entry<String, VDataReader> entry = readers.next();
                        deviceid = entry.getKey();
                        boolean device_not_match= false;
                        for (final Filter filter : filters) {
                            if (COLUMN_DEVICE.equals(filter.references()[0])) {
                                if (!VDataSparkFilters.filter(filter, deviceid)) {
                                    device_not_match = true;
                                    break;
                                }
                            }
                        }

                        if (device_not_match){
                            continue;
                        }
                        vdataReader = entry.getValue();
                        vdataFrame = vdataReader.df();
                        vdataColumns = vdataFrame.cols();

                        if(samplingFrequency == 0){
                            currentVswData = vdataFrame.objects(densifyRowsAhead);
                        }else{
                            currentVswData = vdataFrame.sampling(samplingFrequency);
                        }
                        currentVswIndex =0;
                        return true;
                    }
                    //no suitable VSW found. check next file.
                    readers= null;
                    continue;
                }

                final int lineindex = filename.indexOf('_');
                deviceid = lineindex > 0 ? filename.substring(0, lineindex) : filename;

                boolean device_not_match = false ;
                //device filter
                for (final Filter filter : filters) {
                    if (COLUMN_DEVICE.equals(filter.references()[0])) {
                        if (!VDataSparkFilters.filter(filter, deviceid)) {
                            device_not_match = true;
                            break;
                        }
                    }
                }
                if (device_not_match ){
                    continue;//continue while loop and try next file;
                }

                istream = path.getFileSystem(new Configuration()).open(path);

                if (this.dbcEncoder  == null ){
                    vdataReader = new VDataReader(new LittleEndianSeekableFSReader(istream), targetColumns);
                }else {
                    //DBC Mode...
                    vdataReader = new VDataReader(new LittleEndianSeekableFSReader(istream), vsw_cols);
//                    vdataReader = new VDataReader(new LittleEndianSeekableFSReader(istream), null);
                }

                boolean time_not_match = false;
                //is file's start/time time metadata within the range of the filter
                if (filters.length > 0) {
                    final java.sql.Timestamp storageStartTime = new java.sql.Timestamp(vdataReader.getStartTime());
                    final java.sql.Timestamp storageEndTime = new java.sql.Timestamp(vdataReader.getEndTime());

                    for (final Filter filter : filters) {
                        if (COLUMN_TIME.equals(filter.references()[0])) {
                            if (!VDataSparkFilters.filterWithinTimeRange(filter, storageStartTime, storageEndTime)) {
                                time_not_match = true ;
                                break;
                            }
                        }
                    }
                }

                if(time_not_match ) {
                    continue; //try next file
                }
                vdataFrame = vdataReader.df();
                vdataColumns = vdataFrame.cols();

                if(samplingFrequency == 0){
                    currentVswData = vdataFrame.objects(densifyRowsAhead);
                }else{
                    currentVswData = vdataFrame.sampling(samplingFrequency);
                }
                currentVswIndex =0;
                return true;
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected synchronized void safeCloseResources() {
        if (vdataReader != null) {
            vdataReader.close();
            vdataReader = null;
        }
        if (istream != null) {
            try {
                istream.close();
            } catch (Exception e) {}
            istream = null;
        }
        vdataFrame = null;
        vdataColumns = null;
        vrowIter = null;
    }

    private Record decodeRow(Object [] currentRow ){
        Record currentRecord = new Record() ;

        for (int i=1;i< currentRow.length ;i ++){
            currentRecord.add( "msg_time", new InstantData((Instant) currentRow [0]));
            if (currentRow[i]!= null ){
                MessageContent colInfo = col2msgInfo.get(i-1);
                if (currentRow[i] instanceof  byte []){
                    //column 0 is the InstantObject
                    VswMessage vsw_msg = new VswMessage( (Instant)currentRow [0],colInfo.getChannelID(), colInfo.getMessageID(), (byte[] )currentRow[i]  );
                    dbcDecoder.compute(vsw_msg,vsw_msg, currentRecord, applyFormula);
                }else if (currentRow[i] instanceof  Number []){
                    byte[]data = new byte[ ((Number[]) currentRow[i]).length];
                    Number[] rowdata = (Number[]) currentRow[i];
                    for (int b_index =0 ;b_index  < data.length;b_index ++){
                        data [b_index] =  rowdata[b_index].byteValue();
                    }
                    VswMessage vsw_msg = new VswMessage( (Instant)currentRow [0],colInfo.getChannelID(), colInfo.getMessageID(), data);
                    dbcDecoder.compute(vsw_msg,vsw_msg, currentRecord, applyFormula);
                }
            }
        }
        return  currentRecord;
    }
}
