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
import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConstants.SOURCE_DATA;
import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConstants.SOURCE_DEVICE;
import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConstants.SOURCE_TIME;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.exceeddata.sdk.vdata.data.VDataReader;
import com.exceeddata.sdk.vdata.spark.v3.reader.FSUtils;
import com.exceeddata.sdk.vdata.spark.v3.reader.LittleEndianSeekableFSReader;
import com.exceeddata.sdk.vdata.spark.v3.row.VDataRowDataIterator;
import com.exceeddata.sdk.vdata.spark.v3.row.VDataRowDeviceIterator;
import com.exceeddata.sdk.vdata.spark.v3.row.VDataRowDeviceTimeIterator;
import com.exceeddata.sdk.vdata.spark.v3.row.VDataRowIterator;
import com.exceeddata.sdk.vdata.spark.v3.row.VDataRowTimeIterator;

public final class VDataSparkUtils {
    private VDataSparkUtils() {}
    
    public static VDataRowIterator getRowIterator(final List<String> targetColumns) {
        final int[] columnSources = new int[targetColumns.size()];
        Arrays.fill(columnSources, SOURCE_DATA);
        
        boolean hasDevice = false, hasTime = false;
        for (int i = 0, s = targetColumns.size(); i < s; ++i) {
            final String column = targetColumns.get(i);
            if (COLUMN_DEVICE.equals(column)) {
                hasDevice = true;
                columnSources[i] = SOURCE_DEVICE;
            } else if (COLUMN_TIME.equals(column)) {
                hasTime = true;
                columnSources[i] = SOURCE_TIME;
            }
        }
        
        if (hasDevice) {
            if (hasTime) {
                return new VDataRowDeviceTimeIterator(columnSources);
            } else {
                return new VDataRowDeviceIterator(columnSources);
            }
        } else if (hasTime) {
            return new VDataRowTimeIterator(columnSources);
        } else {
            return new VDataRowDataIterator(); //no need without device/time
        }
    }
    
    public static List<String> getDataColumns (final List<String> targetColumns) {
        final List<String> dataColumns = new ArrayList<>();
        for (final String column : targetColumns) {
            if (!COLUMN_DEVICE.equals(column) && !COLUMN_TIME.equals(column)) {
                dataColumns.add(column);
            }
        }
        return dataColumns;
    }
    
    public static StructType pruneSchema(final StructType originalSchema, final StructType requiredSchema) {
        boolean hasData = false;
        for (int i = 0, s = requiredSchema.size(); i < s; ++i) {
            final String col = requiredSchema.apply(i).name();
            if (!COLUMN_DEVICE.equals(col) && !COLUMN_TIME.equals(col) ) {
                hasData = true;
                break;
            }
        }
        
        //if no columns are provided, then we must preserve original schema due to columnar storage and potential dynamic rows
        //otherwise use the pruned schema
        if (hasData) {
            return requiredSchema;
        }
        
        return originalSchema;
    }
    
    public static StructType readFileSchema(final String filePath, final int maxSchemaRead) {
        try {
            final Configuration conf = new Configuration();
            final List<String> files = FSUtils.listFiles(conf, filePath, true, maxSchemaRead);
            if (files != null && files.size() > 0) {
                final LinkedHashSet<String> targetColumns = new LinkedHashSet<>();
                for (int i = 0, s = files.size(); i < s; ++i) {
                    FSDataInputStream istream = null;
                    LittleEndianSeekableFSReader reader = null;
                    try {
                        final Path path = new Path(files.get(i));
                        istream = path.getFileSystem(conf).open(path);
                        reader = new LittleEndianSeekableFSReader(istream);
                        
                        final String[] names = VDataReader.getNames(reader);
                        targetColumns.addAll(Arrays.asList(names));
                    } finally {
                        if (reader != null) {
                            reader.close();
                        }
                        if (istream != null) {
                            try {
                                istream.close();
                            } catch (IOException e) {}
                        }
                    }
                }

                if (targetColumns.size() > 0) {
                    final ArrayList<String> cols = new ArrayList<>(targetColumns);
                    final ArrayList<StructField> structFields = new ArrayList<>();
                    structFields.add(new StructField(COLUMN_DEVICE, StringType, false, Metadata.empty()));
                    structFields.add(new StructField(COLUMN_TIME, TimestampType, false, Metadata.empty()));
                    for (int i = 0, s = cols.size(); i < s; ++i) {
                        structFields.add(new StructField(cols.get(i), DoubleType, true, Metadata.empty()));
                    }
                    return new StructType(structFields.toArray(new StructField[] {}));
                }
            }
            throw new RuntimeException("no valid files found");
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static int[] getColumnSources(final List<String> targetColumns) {
        final int[] columnSources = new int[targetColumns.size()];
        Arrays.fill(columnSources, SOURCE_DATA);

        for (int i = 0, s = targetColumns.size(); i < s; ++i) {
            final String column = targetColumns.get(i);
            if (COLUMN_DEVICE.equals(column)) {
                columnSources[i] = SOURCE_DEVICE;
            } else if (COLUMN_TIME.equals(column)) {
                columnSources[i] = SOURCE_TIME;
            }
        }

        return columnSources;
    }
}
