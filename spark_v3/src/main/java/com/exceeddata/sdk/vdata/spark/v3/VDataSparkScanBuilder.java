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

import java.util.ArrayList;
import java.util.Map;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class VDataSparkScanBuilder implements ScanBuilder, SupportsPushDownRequiredColumns, SupportsPushDownFilters {

    private StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    private Filter[] pushedFilters = new Filter[] {};
    
    public VDataSparkScanBuilder(
            final StructType schema,
            final Map<String, String> properties,
            final CaseInsensitiveStringMap options) {
        this.schema = schema;
        this.properties = properties;
        this.options = options;
    }
        
    @Override
    public Scan build() {
        return new VDataSparkScan(schema, pushedFilters, properties, options);
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        this.schema = VDataSparkUtils.pruneSchema(this.schema, requiredSchema);
        
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
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
}
