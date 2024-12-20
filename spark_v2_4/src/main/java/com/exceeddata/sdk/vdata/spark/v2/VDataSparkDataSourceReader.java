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

package com.exceeddata.sdk.vdata.spark.v2;

import static com.exceeddata.sdk.vdata.spark.v2.VDataSparkConstants.COLUMN_DEVICE;
import static com.exceeddata.sdk.vdata.spark.v2.VDataSparkConstants.COLUMN_TIME;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.exceeddata.sdk.vdata.spark.v2.message.SignalNameFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.exceeddata.sdk.vdata.spark.v2.reader.FSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VDataSparkDataSourceReader implements DataSourceReader, SupportsPushDownRequiredColumns, SupportsPushDownFilters {
    private static final Logger LOG = LoggerFactory.getLogger(VDataSparkDataSourceReader.class);
    private String filePath = null;
    private StructType schema = null;
    private int schemaNumMaxRead = 1;
    private int densifyRowsAhead = 0;
    private Filter[] pushedFilters = new Filter[] {};

    private String dbcPath = "";
    private boolean applyFormula =true;
    private SignalNameFormatter signalNameFormatter;
    
    public VDataSparkDataSourceReader(final String filePath) {
        LOG.info("VDataSparkDataSourceReader 1");
        this.filePath = filePath;
        this.schema = VDataSparkUtils.readFileSchema(filePath, schemaNumMaxRead);
    }
    
    public VDataSparkDataSourceReader(final String filePath, final int schemaNumMaxRead) {
        LOG.info("VDataSparkDataSourceReader 2");
        this.filePath = filePath;
        this.schemaNumMaxRead = schemaNumMaxRead <= 1 ? 1 : schemaNumMaxRead;
        this.schema = VDataSparkUtils.readFileSchema(filePath, schemaNumMaxRead);
    }
    
    public VDataSparkDataSourceReader(final String filePath, final String targetColumns) {
        LOG.info("VDataSparkDataSourceReader 3");
        this.filePath = filePath;
        
        final String[] columns = targetColumns.split(",");
        final ArrayList<String> cols = new ArrayList<>();
        for (final String c : columns) {
            if (c.trim().length() > 0) {
                cols.add(c.trim());
            }
        }
        if (cols.size() > 0) {
            final int numTargetColumns = cols.size();
            final StructField[] structFields = new StructField[numTargetColumns];
            boolean atLeastOneDataColumn = false;
            for (int i = 0; i < numTargetColumns; ++i) {
                final String col = cols.get(i);
                if (COLUMN_DEVICE.equals(col)) {
                    structFields[i] = new StructField(COLUMN_DEVICE, StringType, false, Metadata.empty());
                } else if (COLUMN_TIME.equals(col)) {
                    structFields[i] = new StructField(COLUMN_TIME, TimestampType, false, Metadata.empty());
                } else {
                    structFields[i] = new StructField(col, DoubleType, true, Metadata.empty());
                    atLeastOneDataColumn = true;
                }
            }

            //if there isn't at least one data column, only device/time, then we should ignore target specification and read all
            if (atLeastOneDataColumn) {
                this.schema = new StructType(structFields);
                return;
            }
        }

        this.schema = VDataSparkUtils.readFileSchema(filePath, schemaNumMaxRead);
    }
    
    public VDataSparkDataSourceReader(final String filePath, final StructType schema) {
        if (filePath == null || filePath.trim().length() == 0) {
            throw new RuntimeException("file path not provided");
        }
        if (schema == null || schema.size() == 0) {
            throw new RuntimeException("schema not provided");
        }
        this.filePath = filePath;

        final int numTargetColumns = schema.size();
        boolean atLeastOneDataColumn = false;
        for (int i = 0; i < numTargetColumns; ++i) {
            final String col = schema.apply(i).name();
            if (!COLUMN_DEVICE.equals(col) && !COLUMN_TIME.equals(col)) {
                atLeastOneDataColumn = true;
                break;
            }
        }
        if (atLeastOneDataColumn) {
            this.schema = schema;
            return;
        }

        this.schema = VDataSparkUtils.readFileSchema(filePath, schemaNumMaxRead);
    }
    
    @Override
    public StructType readSchema() {
        return schema;
    }
    
    /**
     * Get the number of rows to densify.  Default is 0 (no densify).
     * 
     * @return rows
     */
    public int getDensifyRowsAhead() {
        return densifyRowsAhead;
    }
    
    /**
     * Set the number of rows to densify.
     * 
     * @param rows the the number of rows
     */
    public void setDensifyRowsAhead(final int rows) {
        this.densifyRowsAhead = rows > 0 ? rows : 0;
    }

    public void setSignalNameFormatter (SignalNameFormatter signalNameFormatter){
        this.signalNameFormatter = signalNameFormatter;
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        LOG.info("planInputPartitions");
        try {
            final List<InputPartition<InternalRow>> list = new ArrayList<>();
            final Configuration conf = new Configuration();
            final List<String> files = FSUtils.listFiles(conf, filePath, true, Integer.MAX_VALUE);

            List<String > pfiles = null;
            if (files != null && files.size() > 0) {
                for (int i =0 ; i < files.size(); i ++) {
                    String f  = files.get(i);
                    //TODO filter Filename by vin
                    if (i%10 ==0 ) {
                        pfiles = new ArrayList<>();
                        pfiles.add(f);
                        VDataSparkCombinedInputPartition inputPartition = new VDataSparkCombinedInputPartition(pfiles, schema, pushedFilters, densifyRowsAhead, dbcPath, applyFormula, signalNameFormatter);
                        list.add(inputPartition);
                    }else {
                        pfiles.add(f);
                    }
                }
            }
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    @Override
    public void pruneColumns(StructType requiredSchema) {
        this.schema = VDataSparkUtils.pruneSchema(this.schema, requiredSchema);
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        LOG.info("pushFilters");

        if (filters == null || filters.length == 0) {
            if (pushedFilters.length > 0) {
                pushedFilters = new Filter[] {};
            }
            return pushedFilters;
        }
        
        final ArrayList<Filter> pushed = new ArrayList<>();
        final ArrayList<Filter> nonPushed = new ArrayList<>();
        for (final Filter filter : filters) {
            final String[] references = filter.references();
            if (references != null && references.length == 1) {
                if (COLUMN_DEVICE.equals(references[0])) {
                    pushed.add(filter); //once device filter is pushed, then no longer needs to be evaluated
                    continue;
                } 
                if (COLUMN_TIME.equals(references[0])) {
                    pushed.add(filter); //time needs to be both pushed and then evaluated after push
                }
            }
            nonPushed.add(filter);
        }
        pushedFilters = pushed.toArray(new Filter[] {});
        
        return nonPushed.toArray(new Filter[] {});
    }

    @Override
    public Filter[] pushedFilters() {
        return pushedFilters;
    }

    public void setDbcPath(String dbc){
        this.dbcPath = dbc;
    }

    public void setApplyFormula(boolean applyFormula){
        this.applyFormula = applyFormula;
    }

}
