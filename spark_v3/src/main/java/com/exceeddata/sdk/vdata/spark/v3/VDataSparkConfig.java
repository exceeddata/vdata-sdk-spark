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

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import com.exceeddata.sdk.vdata.spark.v3.message.SignalNameFormatter;
import com.exceeddata.sdk.vdata.spark.v3.message.SignalNameFormatterImpl;

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
     * Assign a custom table name
     */
    public static final String TABLE_NAME        = "exceeddata.vdata.spark.table.name";
    
    /**
     * The number of files to read to determine column schema when list columns not provided. Default is 1.
     */
    public static final String SCHEMA_NUM_MAXREAD   = "exceeddata.vdata.spark.schema.num.maxread";
    
    /**
     * The number of rows to look ahead to fill sparse column values. Default is 0 (no densify).
     */
    public static final String DENSIFY_ROWS_AHEAD   = "exceeddata.vdata.spark.densify.rows.ahead";

    public static final String DBC_PATH           = "exceeddata.vdata.spark.dbc.path";

    /**
     * The DBC files path
     */
    public static final String DBC_APPLY_FORMULA           = "exceeddata.vdata.spark.apply.formula";

    /**
     * The DBC files path
     */
    public static final String SIGNAL_NAME_FORMATTER_IMPL           = "exceeddata.vdata.spark.signal.name.formatter";

    /**
     *
     */
    public static final String SAMPLING_FREQUENCY           = "exceeddata.vdata.spark.sampling.frequency";

    /**
     * The batch number of vsw
     */
    public static final String VSW_BATCH           = "exceeddata.vdata.spark.vsw.batch";
    
    private VDataSparkConfig() {}
    
    public static String getInputPath(final CaseInsensitiveStringMap options) {
        return options.get(INPUT_PATH);
    }
        
    public static String getInputColumns(final CaseInsensitiveStringMap options) {
        return options.get(INPUT_COLUMNS);
    }
    
    public static String getTableName(final CaseInsensitiveStringMap options) {
        return options.get(TABLE_NAME);
    }
    
    public static int getSchemaNumMaxRead(final CaseInsensitiveStringMap options) {
        final int numMaxRead = options.getInt(SCHEMA_NUM_MAXREAD, 1);
        return numMaxRead > 1 ? numMaxRead : 1;
    }
    
    public static int getDensifyRowsAhead(final CaseInsensitiveStringMap options) {
        final int densifyRowsAhead = options.getInt(DENSIFY_ROWS_AHEAD, 0);
        return densifyRowsAhead > 0 ? densifyRowsAhead : 0;
    }

    public static String getDbcPath(final CaseInsensitiveStringMap options) {
        return options.get(DBC_PATH);
    }

    public static boolean getApplyFormula(final CaseInsensitiveStringMap options) {
        return  options.getBoolean(DBC_APPLY_FORMULA, true);
    }

    public static SignalNameFormatter getSignalNameFormatter(final CaseInsensitiveStringMap options){
        String clsName = options.get(SIGNAL_NAME_FORMATTER_IMPL);
        try {
            if (StringUtils.isBlank(clsName)) {
                clsName = "com.exceeddata.sdk.vdata.spark.v3.message.SignalNameFormatterImpl";
            }

            return (SignalNameFormatter)Class.forName(clsName).newInstance();
        }catch ( Exception e ){
            return new SignalNameFormatterImpl();
        }
    }

    public static int getSamplingFrequency(final CaseInsensitiveStringMap options) {
        return options.getInt(SAMPLING_FREQUENCY, 0);
    }

    public static int getVswBatch(final CaseInsensitiveStringMap options) {
        return options.getInt(VSW_BATCH, 10);
    }
}
