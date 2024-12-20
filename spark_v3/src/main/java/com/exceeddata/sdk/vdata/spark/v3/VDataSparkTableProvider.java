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

import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConfig.getInputColumns;
import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConfig.getInputPath;
import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConfig.getSchemaNumMaxRead;
import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConstants.COLUMN_DEVICE;
import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConstants.COLUMN_TIME;

import java.util.ArrayList;
import java.util.Map;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class VDataSparkTableProvider implements TableProvider {
    
    public VDataSparkTableProvider() {
        
    }
    
    @Override
    public StructType inferSchema(final CaseInsensitiveStringMap options) {
        final String inputColumns = getInputColumns(options);
        if (inputColumns != null && inputColumns.trim().length() > 0) {
            final String[] colnames = inputColumns.split(",");
            final ArrayList<String> cols = new ArrayList<>();
            for (final String colname : colnames) {
                final String name = colname.trim();
                if (name.length() > 0) {
                    cols.add(name);
                }
            }
            
            final int size = cols.size();
            if (size == 0) {
                return null;
            }
            
            final StructField[] fields = new StructField[size];
            for (int i = 0; i < size; ++i) {
                final String name = cols.get(i);
                if (COLUMN_DEVICE.equals(name)) {
                    fields[i] = new StructField(name, DataTypes.StringType, false, Metadata.empty());
                } else if (COLUMN_TIME.equals(name)) {
                    fields[i] = new StructField(name, DataTypes.TimestampType, false, Metadata.empty());
                } else {
                    fields[i] = new StructField(name, DataTypes.DoubleType, true, Metadata.empty());
                }
            }
            return new StructType(fields);
        }
        
        final int schemaNumMaxRead = getSchemaNumMaxRead(options);
        final String filePath = getInputPath(options);
        if (filePath == null || filePath.trim().length() == 0) {
            throw new RuntimeException("input path not provided");
        }
        
        return VDataSparkUtils.readFileSchema(filePath, schemaNumMaxRead);
        
    }

    @Override
    public Table getTable(
            final StructType schema, 
            final Transform[] partitioning, 
            final Map<String, String> properties) {
        return new VDataSparkTable(schema, partitioning, properties);
    }

    @Override
    public boolean supportsExternalMetadata() {
        return false;
    }
}
