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

import static com.exceeddata.sdk.vdata.spark.v2.VDataSparkConfig.getDensifyRowsAhead;
import static com.exceeddata.sdk.vdata.spark.v2.VDataSparkConfig.getInputColumns;
import static com.exceeddata.sdk.vdata.spark.v2.VDataSparkConfig.getInputPath;
import static com.exceeddata.sdk.vdata.spark.v2.VDataSparkConfig.getSchemaNumMaxRead;
import static com.exceeddata.sdk.vdata.spark.v2.VDataSparkConfig.getDbcPath;
import static com.exceeddata.sdk.vdata.spark.v2.VDataSparkConfig.getApplyFormula;
import static com.exceeddata.sdk.vdata.spark.v2.VDataSparkConfig.getSignalNameFormatter;

import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;

public class VDataSparkDataSource implements DataSourceV2, ReadSupport {
    
    public VDataSparkDataSource() {
        
    }
    
    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        final String path = getInputPath(options);
        if (path == null || path.trim().length() == 0) {
            throw new RuntimeException("input path not provided");
        }

        final String cols = getInputColumns(options);
        final int maxread = getSchemaNumMaxRead(options);
        final int densifyRowsAhead = getDensifyRowsAhead(options);
        final VDataSparkDataSourceReader reader;
        
        if (cols != null && cols.trim().length() > 0) {
            reader = new VDataSparkDataSourceReader(path, cols);
        } else {
            reader = new VDataSparkDataSourceReader(path, maxread);
        }
        reader.setDensifyRowsAhead(densifyRowsAhead);
        reader.setDbcPath(getDbcPath(options));
        reader.setApplyFormula(getApplyFormula(options));
        reader.setSignalNameFormatter( getSignalNameFormatter(options));
        
        return reader;
    }
}
