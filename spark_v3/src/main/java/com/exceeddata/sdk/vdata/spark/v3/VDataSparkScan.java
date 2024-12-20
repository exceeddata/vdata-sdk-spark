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

import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConfig.getInputPath;
import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConfig.getSchemaNumMaxRead;

import java.util.Map;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class VDataSparkScan implements Scan {
    private StructType schema;
    private Filter[] filters;
    private Map<String, String> properties;
    private CaseInsensitiveStringMap options;

    private int enter = 0;
    
    public VDataSparkScan(
            final StructType schema,
            final Filter[] filters,
            final Map<String, String> properties,
            final CaseInsensitiveStringMap options) {
        this.schema = schema;
        this.filters = filters;
        this.properties = properties;
        this.options = options;
    }
    
    @Override
    public StructType readSchema() {
        if (schema != null) {
            return schema;
        }

        final int schemaNumMaxRead = getSchemaNumMaxRead(options);
        final String filePath = getInputPath(options);
        if (filePath == null || filePath.trim().length() == 0) {
            throw new RuntimeException("input path not provided");
        }
        
        schema = VDataSparkUtils.readFileSchema(filePath, schemaNumMaxRead);
        
        return schema;
    }

    @Override
    public String description() {
        return "vdata_spark_table";
    }

    @Override
    public Batch toBatch() {
        System.out.println("VDataSparkScan" + this);
        enter++;
        return new VDataSparkBatch(schema, filters, properties, options, enter);
    }
}
