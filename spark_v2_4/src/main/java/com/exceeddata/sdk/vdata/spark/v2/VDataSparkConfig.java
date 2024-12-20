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

import java.util.Optional;

import com.exceeddata.sdk.vdata.spark.v2.message.SignalNameFormatter;
import com.exceeddata.sdk.vdata.spark.v2.message.SignalNameFormatterImpl;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

public final class VDataSparkConfig {
    
    /**
     * The input path to VData files.  It is required.
     */
    public static final String INPUT_PATH           = "exceeddata.vdata.spark.input.path";
    
    /**
     * The column schema to read from vdata files.
     */
    public static final String INPUT_COLUMNS        = "exceeddata.vdata.spark.input.columns";
    
    /**
     * The number of files to read to determine column schema when list columns not provided. Default is 1.
     */
    public static final String SCHEMA_NUM_MAXREAD   = "exceeddata.vdata.spark.schema.num.maxread";
    
    /**
     * The number of rows to look ahead to fill sparse column values. Default is 0 (no densify).
     */
    public static final String DENSIFY_ROWS_AHEAD   = "exceeddata.vdata.spark.densify.rows.ahead";


    /**
     * The DBC files path
     */
    public static final String DBC_PATH           = "exceeddata.vdata.spark.dbc.path";


    /**
     * The DBC files path
     */
    public static final String DBC_APPLY_FORMULA           = "exceeddata.vdata.spark.apply.formula";


    /**
     * The DBC files path
     */
    public static final String SIGNAL_NAME_FORMATTER_IMPL           = "exceeddata.vdata.spark.signal.name.formatter";
    private VDataSparkConfig() {}
    
    public static String getInputPath(final DataSourceOptions options) {
        final Optional<String> path = options.get(INPUT_PATH);
        if (path == null || !path.isPresent()) {
            return null;
        }
        return path.get();
    }
    
    public static String getInputColumns(final DataSourceOptions options) {
        final Optional<String> columns = options.get(INPUT_COLUMNS);
        if (columns == null || !columns.isPresent()) {
            return null;
        }
        return columns.get();
    }

    public static int getSchemaNumMaxRead(final DataSourceOptions options) {
        final int schemaNumMaxRead = options.getInt(SCHEMA_NUM_MAXREAD, 1);
        return schemaNumMaxRead > 1 ? schemaNumMaxRead : 1;
    }

    public static int getDensifyRowsAhead(final DataSourceOptions options) {
        final int densifyRowsAhead = options.getInt(DENSIFY_ROWS_AHEAD, 0);
        return densifyRowsAhead > 0 ? densifyRowsAhead : 0;
    }


    public static String getDbcPath(final DataSourceOptions options) {
        final Optional<String> path = options.get(DBC_PATH);
        if (path == null || !path.isPresent()) {
            return null;
        }
        return path.get();
    }

    public static boolean getApplyFormula(final DataSourceOptions options) {
        return  options.getBoolean(DBC_APPLY_FORMULA, true);
    }

    public static SignalNameFormatter getSignalNameFormatter(final DataSourceOptions options){
        final Optional<String> impl = options.get(SIGNAL_NAME_FORMATTER_IMPL);
        try {
            String clsName;
            if (impl == null || !impl.isPresent()) {
                clsName = "com.exceeddata.sdk.vdata.spark.v2.message.SignalNameFormatterImpl";
            }else {
                clsName = impl.get();
            }

            return (SignalNameFormatter)Class.forName(clsName).newInstance();
        }catch ( Exception e ){
            return new SignalNameFormatterImpl();
        }

    }
}
