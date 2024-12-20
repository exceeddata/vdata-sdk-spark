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

package com.exceeddata.sdk.vdata.spark.v3.row;

import static com.exceeddata.sdk.vdata.spark.v3.VDataSparkConstants.SOURCE_DATA;

import java.time.Instant;

import com.exceeddata.sdk.vdata.data.VDataRow;

public class VDataRowTimeIterator implements VDataRowIterator {

    private final int[] columnSources;
    private final int length;
    
    private Object[] _values;
    private Instant _time;
    
    private int index = 0;
    private int position = 0;
    
    public VDataRowTimeIterator(final int[] columnSources) {
        this.columnSources = columnSources;
        this.length = columnSources.length;
    }
    
    @Override
    public void set(final String deviceid, final VDataRow vrow) {
        this._values = vrow.getValues();
        this._time = vrow.getTime();
        this.index = 0;
        this.position = 0;
    }
    
    public VDataRowTimeIterator newInstance() {
        return new VDataRowTimeIterator(columnSources);
    }
    
    @Override
    public boolean hasNext() {
        return index < length;
    }

    @Override
    public Object next() {
        if (columnSources[index++] == SOURCE_DATA) {
            return _values[position++];
        }
        
        //spark timestamp is microsecond precision, use big integer to avoid java conversion to double
        //need to separate the if/else to previoud java from autoboxing to double
        return  _time.getEpochSecond() * 1000000 + _time.getNano() / 1000; 
    }

}
