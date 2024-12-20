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

package com.exceeddata.sdk.vdata.spark.v3.reader;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FSDataInputStream;

import com.exceeddata.sdk.vdata.binary.BinaryEOFException;
import com.exceeddata.sdk.vdata.binary.BinaryLittleEndianUtils;
import com.exceeddata.sdk.vdata.binary.BinarySeekableReader;

/**
 * Little Endian Binary FSDataInputStream Reader
 *
 */
public class LittleEndianSeekableFSReader implements BinarySeekableReader {
    private static final long serialVersionUID = 1L;
    
    private FSDataInputStream seekable = null;
    private byte[] data = new byte[1024]; //max bytes data, bigger than this we use dynamic allocation.
    
    public LittleEndianSeekableFSReader(final FSDataInputStream seekable) {
        this.seekable = seekable;
    }
    
    public LittleEndianSeekableFSReader(LittleEndianSeekableFSReader reader) {
        this.seekable = reader.seekable;
    }
    
    /** {@inheritDoc} */
    @Override
    public LittleEndianSeekableFSReader clone() {
        return new LittleEndianSeekableFSReader(this);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] readBytes(int len) throws IOException {
        final byte[] data;
        if (len <= 1024) {
            data = this.data;
            Arrays.fill(data, (byte) 0);
        } else {
            data = new byte[len];
        }
        read(data, len);
        
        return BinaryLittleEndianUtils.bytes(data, 0, len);
    }
    
    private void read(final byte[] data, final int len) throws IOException {
        int remaining = len, offset = 0, read;
        do {
            read = seekable.read(data, offset, remaining);
            if (read < 0) {
                throw new BinaryEOFException();
            }
            remaining -= read;
            offset += read;
        } while (remaining > 0);
    }
    
    /** {@inheritDoc} */
    @Override
    public void seek(final long pos) throws IOException {
        seekable.seek(pos);
    }
    
    /** {@inheritDoc} */
    @Override
    public void skipBytes(final int len) throws IOException {
        int remaining = len, skipped;
        do {
            skipped = seekable.skipBytes(remaining);
            if (skipped <= 0) {
                throw new BinaryEOFException();
            }
            remaining -= skipped;
        } while (remaining > 0);
    }
    
    /** {@inheritDoc} */
    @Override
    public void close()  {
        if (seekable != null) {
            try {
                seekable.close();
            } catch (IOException e) {
            }
            seekable = null;
        }
    }
    
    /** {@inheritDoc} */
    @Override
    public long getPos() throws IOException {
        return seekable.getPos();
    }
}
