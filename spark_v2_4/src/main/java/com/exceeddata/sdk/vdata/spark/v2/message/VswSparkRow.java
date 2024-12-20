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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;


/**
 * Spark Row Object mapping user selected columns to object row
 */
public class VswSparkRow extends InternalRow {
    private static final long serialVersionUID = 8555862791942968865L;
    
    protected final UTF8String deviceId;
    protected final Object[] rowData;
    protected final List<String> targetColumns;
    protected final int [] col_index_mapping;
    
    public VswSparkRow(final String deviceId, final Object [] rowData , final List<String> targetColumns, final int [] col_index_mapping){
        this.deviceId = UTF8String.fromString(deviceId);
        this.rowData = rowData;
        this.col_index_mapping = col_index_mapping;
        this.targetColumns = targetColumns;
    }

    @Override
    public int numFields() {
        return targetColumns.size();
    }

    @Override
    public void setNullAt(int i) {
        throw new RuntimeException("Modification to VswSparkRow Object is not supported");
    }

    @Override
    public void update(int i, Object value) {
        throw new RuntimeException("Modification to VswSparkRow Object is not supported");
    }

    @Override
    public InternalRow copy() {
        return new VswSparkRow(deviceId.toString(), rowData, null, col_index_mapping);
    }

    @Override
    public boolean isNullAt(int i) {
        return  get(i,null) ==null;
    }

    @Override
    public boolean getBoolean(int i) {
        Object data = get(i,null);
        return data ==null ? null: (Boolean)data ;
    }

    @Override
    public byte getByte(int i) {
        Object data = get(i,null);
        return data ==null ? null: (Byte)data ;
    }

    @Override
    public short getShort(int i) {
        Object data = get(i,null);
        return data ==null ? null: (Short)data ;
    }

    @Override
    public int getInt(int i) {
        Object data = get(i,null);
        return data ==null ? null: (Integer)data ;
    }

    @Override
    public long getLong(int i) {
        Object data = get(i,null);
        return data ==null ? 0: (Long)data ;
    }

    @Override
    public float getFloat(int i) {
        Object data = get(i,null);
        return data ==null ? null: (Float)data ;
    }

    @Override
    public double getDouble(int i) {
        Object data = get(i,null);

        if (data == null ){
            return 0;
        }
        if (data instanceof  Double ) {
            return data == null ? null : (Double) data;
        }else if (data instanceof  Long ) {
            return ((Long)data).longValue();
        }

        return 0;
    }

    @Override
    public Decimal getDecimal(int i, int i1, int i2) {
        return null;
    }

    @Override
    public UTF8String getUTF8String(int i) {
        Object data = get(i,null);
        return data ==null ? null: (UTF8String)data ;
    }

    @Override
    public byte[] getBinary(int i) {
        return (byte[])get(i,null);
    }

    @Override
    public CalendarInterval getInterval(int i) {
        //not supported
        return null;
    }

    @Override
    public InternalRow getStruct(int i, int i1) {
        //not supported
        return null;
    }

    @Override
    public ArrayData getArray(int i) {
        //not supported
        return null;
    }

    @Override
    public MapData getMap(int i) {
        //not supported
        return null;
    }

    @Override
    public Object get(int i, DataType dataType) {

        if (col_index_mapping [i] ==SOURCE_DEVICE){
            return  deviceId;
        }

        if(col_index_mapping [i ]==SOURCE_TIME){
            Instant _time = (Instant) rowData[0];
            return  _time.getEpochSecond() * 1000000 + _time.getNano() / 1000;
        }
        return rowData [i+1];
    }
}
