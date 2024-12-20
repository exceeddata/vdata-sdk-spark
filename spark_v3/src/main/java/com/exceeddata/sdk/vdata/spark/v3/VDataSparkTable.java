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

import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConfig.INPUT_PATH;
import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConfig.TABLE_NAME;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;


public class VDataSparkTable implements SupportsRead {

    private StructType schema;
    private Map<String, String> properties;
    private Set<TableCapability> capabilities;
    private String name;
    
    public VDataSparkTable(
            final StructType schema,
            final Transform[] partitioning, 
            final Map<String, String> properties) {
        this.schema = schema;
        this.properties = properties;
        this.name = properties.get(TABLE_NAME);
        if (this.name == null || this.name.trim().length() == 0) {
            final String inputPath = properties.get(INPUT_PATH);
            if (inputPath == null || inputPath.trim().length() == 0) {
                throw new RuntimeException("input path not provided");
            }
            
//            try {
//                final Path folderPath = FSUtils.getFolderPath(conf, inputPath.trim());
//                this.name = folderPath.getName();
//                if (this.name == null || this.name.trim().length() == 0) {
//                    throw new RuntimeException("input path not valid");
//                }
//            } catch (IOException e) {
//                throw new RuntimeException(e.getMessage(), e);
//            }

            // 支持多个文件输入
            if(inputPath == null || inputPath.trim().length() == 0){
                throw new RuntimeException("input path not valid");
            }
        }
    }
    
    @Override
    public String name() {
        return name;
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        if (capabilities == null) {
            this.capabilities = new HashSet<>();
            capabilities.add(TableCapability.BATCH_READ);
        }
        return capabilities;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        if (options.isEmpty()){
            //for TableCatalog Mode, the option is empty...
            options= new CaseInsensitiveStringMap(properties);
        }
        return new VDataSparkScanBuilder(schema, properties, options);
    }
}
