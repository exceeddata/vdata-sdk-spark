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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

import com.exceeddata.sdk.vdata.data.VDataFrame;
import com.exceeddata.sdk.vdata.data.VDataReader;
import com.exceeddata.sdk.vdata.data.VDataRow;
import com.exceeddata.sdk.vdata.spark.v3.reader.LittleEndianSeekableFSReader;
import com.exceeddata.sdk.vdata.spark.v3.row.VDataRowIterator;

import scala.collection.JavaConverters;

public class VDataSparkPartitionReader implements PartitionReader<InternalRow> {

    private String filePath = null;
    private int densifyRowsAhead = 0;
    private List<String> targetColumns = null;
    private FSDataInputStream istream = null;
    private Filter[] filters = null;
    
    protected VDataReader vdataReader = null;
    protected VDataFrame vdataFrame = null;
    protected List<String> vdataColumns = null;
    protected Iterator<VDataRow> vrowIter = null;
    protected String deviceid = null;

    private VDataRow vrow = null;
    private VDataRowIterator templateIterator = null;
    
    public VDataSparkPartitionReader(final VDataSparkInputPartition inputPartition) {
        this(inputPartition.getFilePath(), inputPartition.getSchema(), inputPartition.getFilters(), inputPartition.getDensifyRowsAhead());
    }
    
    public VDataSparkPartitionReader(final String filePath, final StructType schema, final Filter[] filters, final int densifyRowsAhead) {
        this.filePath = filePath;
        this.filters = filters;
        this.densifyRowsAhead = densifyRowsAhead > 0 ? densifyRowsAhead : 0;
        
        if (schema != null && schema.size() > 0) {
            targetColumns = Arrays.asList(schema.fieldNames());
            templateIterator = VDataSparkUtils.getRowIterator(targetColumns);
            targetColumns = VDataSparkUtils.getDataColumns(targetColumns);
        }
        if (targetColumns == null || targetColumns.size() == 0) {
            throw new RuntimeException("schema not provided");
        }
        
        initialize();
    }
    
    @Override
    public void close() throws IOException {
        safeCloseResources();
    }

    @Override
    public boolean next() throws IOException {
        if (vrowIter != null && vrowIter.hasNext()) {
            vrow = vrowIter.next();
            return true;
        }
        return false;
    }

    @Override
    public InternalRow get() {
        final VDataRowIterator iter = templateIterator.newInstance();
        iter.set(deviceid, vrow);
        
        return InternalRow.apply(JavaConverters.asScalaIteratorConverter(iter).asScala().toSeq());
    }
    
    public List<String> getTargetColumns() {
        return targetColumns;
    }
    
    public void setTargetColumns(final List<String> targetColumns) {
        this.targetColumns = targetColumns;
    }
    
    @SuppressWarnings("deprecation")
    protected synchronized void initialize() {
        try {
            /*
             * file name should be device_rollout_datetime.extension format
             * 
             * also we could store device in extended info array at customization
             */
            
            final Path path = new Path(filePath);
            final String pathname = path.getName().trim();
            final int extindex = pathname.lastIndexOf('.');
            final String filename = extindex > 0 ? pathname.substring(0, extindex) : pathname;
            
            final int lineindex = filename.indexOf('_');
            deviceid = lineindex > 0 ? filename.substring(0, lineindex) : filename;
            
            //device filter
            for (final Filter filter : filters) {
                if (COLUMN_DEVICE.equals(filter.references()[0])) {
                    if (!VDataSparkFilters.filter(filter, deviceid)) {
                        return;
                    }
                }
            }
            
            istream = path.getFileSystem(new Configuration()).open(path);
            vdataReader = new VDataReader(new LittleEndianSeekableFSReader(istream), targetColumns);
            
            //is file's start/time time metadata within the range of the filter
            if (filters.length > 0) {
                final java.sql.Timestamp storageStartTime = new java.sql.Timestamp(vdataReader.getStartTime());
                final java.sql.Timestamp storageEndTime = new java.sql.Timestamp(vdataReader.getEndTime());
            
                for (final Filter filter : filters) {
                    if (COLUMN_TIME.equals(filter.references()[0])) {
                        if (!VDataSparkFilters.filterWithinTimeRange(filter, storageStartTime, storageEndTime)) {
                            return;
                        }
                    }
                }
            }
            
            vdataFrame = vdataReader.df();
            vdataColumns = vdataFrame.cols();
            vrowIter = vdataFrame.iterator(densifyRowsAhead);
            
            
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
}
