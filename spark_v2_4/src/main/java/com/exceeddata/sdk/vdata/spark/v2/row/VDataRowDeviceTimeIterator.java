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

package com.exceeddata.sdk.vdata.spark.v2.row;

import static com.exceeddata.sdk.vdata.spark.v2.VDataSparkConstants.SOURCE_DATA;
import static com.exceeddata.sdk.vdata.spark.v2.VDataSparkConstants.SOURCE_TIME;

import java.time.Instant;

import org.apache.spark.unsafe.types.UTF8String;

import com.exceeddata.sdk.vdata.data.VDataRow;

public class VDataRowDeviceTimeIterator implements VDataRowIterator {

    private final int[] columnSources;
    private final int length;
    
    private UTF8String deviceid;
    private Object[] _values;
    private Instant _time;
    
    private int index = 0;
    private int position = 0;
    
    public VDataRowDeviceTimeIterator(final int[] columnSources) {
        this.columnSources = columnSources;
        this.length = columnSources.length;
    }
    
    @Override
    public void set(final String deviceid, final VDataRow vrow) {
        this.deviceid = UTF8String.fromString(deviceid);
        this._values = vrow.getValues();
        this._time = vrow.getTime();
        this.index = 0;
        this.position = 0;
    }
    
    public VDataRowDeviceTimeIterator newInstance() {
        return new VDataRowDeviceTimeIterator(columnSources);
    }
    
    @Override
    public boolean hasNext() {
        return index < length;
    }

    @Override
    public Object next() {
        final int t = columnSources[index++];
        if (t == SOURCE_DATA) {
            return _values[position++];
        }
        
        //spark timestamp is micro-second precision
        //break if/else to prevent Java from autoboxing to double
        if (t == SOURCE_TIME) {
            return  _time.getEpochSecond() * 1000000 + _time.getNano() / 1000;
        }
        
        return deviceid;
    }

}
