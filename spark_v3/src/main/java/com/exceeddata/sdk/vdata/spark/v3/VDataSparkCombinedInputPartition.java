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

import java.util.List;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

import com.exceeddata.sdk.vdata.spark.v3.message.SignalNameFormatter;

public class VDataSparkCombinedInputPartition implements InputPartition {
    private static final long serialVersionUID = -850594035723899609L;

    private final List<String> filePath;
    private final StructType schema;
    private final Filter[] filters;
    private final int densifyRowsAhead;

    private final String dbcPaths;

    private final boolean applyFormula;
    private SignalNameFormatter signalNameFormatter;

    private int samplingFrequency;

    public VDataSparkCombinedInputPartition(final List<String> filePath,
                                            final StructType schema,
                                            final Filter[] filters,
                                            final int densifyRowsAhead,
                                            final String dbcPaths,
                                            final boolean applyFormula,
                                            final SignalNameFormatter signalNameFormatter,
                                            final int samplingFrequency) {
        this.filePath = filePath;
        this.schema = schema;
        this.filters = filters;
        this.densifyRowsAhead = densifyRowsAhead;
        this.dbcPaths = dbcPaths;
        this.applyFormula = applyFormula;
        this.signalNameFormatter =  signalNameFormatter;
        this.samplingFrequency = samplingFrequency;
    }

    public List<String> getFilePath() {
        return filePath;
    }

    public StructType getSchema() {
        return schema;
    }

    public Filter[] getFilters() {
        return filters;
    }

    public int getDensifyRowsAhead() {
        return densifyRowsAhead;
    }

    public String getDbcPaths() {
        return dbcPaths;
    }

    public boolean getApplyFormula() {
        return applyFormula;
    }

    public SignalNameFormatter getSignalNameFormatter() {
        return signalNameFormatter;
    }

    public int getSamplingFrequency() {return samplingFrequency;}
}
