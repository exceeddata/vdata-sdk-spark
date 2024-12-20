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

package com.exceeddata.sdk.vdata.spark.v3.message;

import java.time.Instant;

import com.exceeddata.ac.common.message.MessageContent;
import com.exceeddata.ac.common.message.MessageDesc;
import com.exceeddata.ac.common.message.MessageDirection;

/**
 * Class for Message Decode
 */
public class VswMessage implements MessageContent, MessageDesc {
    private static final long serialVersionUID = -3151997063511664392L;
    
    private Instant msg_time;
    private int channelId;
    private long messageId;
    private byte [] data;

    public VswMessage (Instant time, int channelId, long messageId, byte [] data) {
        this.msg_time = time;
        this.channelId = channelId;
        this.messageId = messageId;
        this.data = data;
    }

    @Override
    public int getChannelID() {
        return channelId;
    }

    @Override
    public long getMessageID() {
        return messageId;
    }

    @Override
    public boolean isError() {
        return false;
    }


    @Override
    public Instant getTimeStart() {
        return msg_time;
    }

    @Override
    public long getNanosOffset() {
        //nano offset comparing to getTimeStart.
        //we use one class for meta/message, so offset is always 0
        return 0;
    }

    @Override
    public int getDataLength() {
        return data.length;
    }

    @Override
    public MessageDirection getDirection() {
        return null;
    }

    @Override
    public byte[] getData() {
        return data;
    }


}
