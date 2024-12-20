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

package com.exceeddata.sdk.vdata.spark.v2.message;

import static com.exceeddata.sdk.vdata.spark.v2.VDataSparkConstants.SOURCE_DEVICE;
import static com.exceeddata.sdk.vdata.spark.v2.VDataSparkConstants.SOURCE_TIME;

import java.time.Instant;
import java.util.List;

import org.apache.spark.sql.types.DataType;

import com.exceeddata.ac.common.data.record.Record;

public class VswSparkRawDataRow extends VswSparkRow {
    private static final long serialVersionUID = 3926136196653323188L;
    
    private final Record record;
    
    public VswSparkRawDataRow(
            final String deviceId,
            final Record record,
            final List<String> targetColumns,
            final int[] col_index_mapping) {
        super(deviceId, new Object[0], targetColumns, col_index_mapping);
        
        this.record = record;
    }

    @Override
    public Object get(int i, DataType dataType) {
        try {
            if (col_index_mapping[i] == SOURCE_DEVICE) {
                return deviceId;
            }

            if (col_index_mapping[i] == SOURCE_TIME) {
                Instant _time = record.get("msg_time").toInstant();
                return _time.getEpochSecond() * 1000000 + _time.getNano() / 1000;
            }
            return record.get(targetColumns.get(i)).getObject();
        } catch (Exception e){
            return null;
        }
    }
}
