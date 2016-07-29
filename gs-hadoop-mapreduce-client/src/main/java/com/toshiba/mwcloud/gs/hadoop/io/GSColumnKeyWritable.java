/*
   Copyright (c) 2016 TOSHIBA CORPORATION.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.toshiba.mwcloud.gs.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;

import com.toshiba.mwcloud.gs.GSType;

/**
 * <div lang="ja">
 * GridDBのロウキー用Writableです。<br/>
 * GSRowRecirdReaderが作成し、Mapタスクにキーとして渡されます。
 * </div><div lang="en">
 * Writable for row key of GridDB container created by GSRowRecirdReader and handed over as an input key to the Map task.
 * </div>
 */
public class GSColumnKeyWritable implements WritableComparable<GSColumnKeyWritable> {
    protected GSType type_;
    protected Object value_;

    protected static final byte NULL = 0x00;
    protected static final byte INTEGER = 0x06;
    protected static final byte LONG = 0x07;
    protected static final byte STRING = 0x09;
    protected static final byte TIMESTAMP = 0x0a;

    /**
     * <div lang="ja">
     * 指定された型のオブジェクトを作成します。
     * @param type キーの型(INTEGER/LONG/STRING/TIMESTAMP)
     * </div><div lang="en">
     * Create object of the specified data type.
     * @param type data type of key (INTEGER/LONG/STRING/TIMESTAMP)
     * </div>
     */
    public GSColumnKeyWritable(GSType type) {
        type_ = type;
        value_ = null;
    }
    /**
     * <div lang="ja">
     * ロウキーの型を返します。
     * @return ロウキーの型
     * </div><div lang="en">
     * Return the data type of the key.
     * @return data type of key
     * </div>
     */
    public GSType getType() {
        return type_;
    }
    /**
     * <div lang="ja">
     * ロウキーの値を返します。
     * @return ロウキーの値
     * </div><div lang="en">
     * Return the value of the key.
     * @return value of key
     * </div>
     */
    public Object getValue() {
        return value_;
    }
    /**
     * <div lang="ja">
     * STRING型ロウキーの値を返します。
     * @return STRING型ロウキーの値
     * </div><div lang="en">
     * Return the value of the STRING key.
     * @return value of STRING key
     * </div>
     */
    public String getString() {
        return (String) value_;
    }
    /**
     * <div lang="ja">
     * INTEGER型ロウキーの値を返します。
     * @return INTEGER型ロウキーの値
     * </div><div lang="en">
     * Return the value of the INTEGER key.
     * @return value of INTEGER key
     * </div>
     */
    public Integer getInteger() {
        return (Integer) value_;
    }
    /**
     * <div lang="ja">
     * LONG型ロウキーの値を返します。
     * @return LONG型ロウキーの値
     * </div><div lang="en">
     * Return the value of the LONG key.
     * @return value of LONG key
     * </div>
     */
    public Long getLong() {
        return (Long) value_;
    }
    /**
     * <div lang="ja">
     * TIMESTAMP型ロウキーの値を返します。
     * @return TIMESTAMP型ロウキーの値
     * </div><div lang="en">
     * Return the value of the TIMESTAMP key.
     * @return value of TIMESTAMP key
     * </div>
     */
    public Date getTimestamp() {
        return (Date) value_;
    }
    /**
     * <div lang="ja">
     * ロウキーの値を設定します。
     * @param value ロウキーの値
     * </div><div lang="en">
     * Set the value of the key.
     * @param value value of key
     * </div>
     */
    public void setValue(Object value) {
        value_ = value;
    }
    /**
     * <div lang="ja">
     * STRING型ロウキーの値を設定します。
     * @param value STRING型ロウキーの値
     * </div><div lang="en">
     * Set the value of the STRING key.
     * @param value value of STRING key
     * </div>
     */
    public void setString(String value) {
        type_ = GSType.STRING;
        value_ = value;
    }
    /**
     * <div lang="ja">
     * INTEGER型ロウキーの値を設定します。
     * @param value INTEGER型ロウキーの値
     * </div><div lang="en">
     * Set the value of the INTEGER key.
     * @param value value of INTEGER key
     * </div>
     */
    public void setInteger(Integer value) {
        type_ = GSType.INTEGER;
        value_ = value;
    }
    /**
     * <div lang="ja">
     * LONG型ロウキーの値を設定します。
     * @param value LONG型ロウキーの値
     * </div><div lang="en">
     * Set the value of the LONG key.
     * @param value value of LONG key
     * </div>
     */
    public void setLong(Long value) {
        type_ = GSType.LONG;
        value_ = value;
    }
    /**
     * <div lang="ja">
     * TIMESTAMP型ロウキーの値を設定します。
     * @param value TIMESTAMP型ロウキーの値
     * </div><div lang="en">
     * Set the value of the TIMESTAMP key.
     * @param value value of TIMESTAMP key
     * </div>
     */
    public void setTimestamp(Date value) {
        type_ = GSType.TIMESTAMP;
        value_ = value;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        byte type = in.readByte();
        switch (type) {
        case INTEGER:
            type_ = GSType.INTEGER;
            value_ = in.readInt();
            break;
        case LONG:
            type_ = GSType.LONG;
            value_ = in.readLong();
            break;
        case STRING:
            type_ = GSType.STRING;
            value_ = readString(in);
            break;
        case TIMESTAMP:
            type_ = GSType.TIMESTAMP;
            value_ = new Date(in.readLong());
            break;
        default:
            type_ = null;
            value_ = null;
            break;
        }
    }
    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        switch (type_) {
        case INTEGER:
            out.writeByte(INTEGER);
            out.writeInt((Integer) value_);
            break;
        case LONG:
            out.writeByte(LONG);
            out.writeLong((Long) value_);
            break;
        case STRING:
            out.writeByte(STRING);
            writeString(out, (String) value_);
            break;
        case TIMESTAMP:
            out.writeByte(TIMESTAMP);
            out.writeLong(((Date) value_).getTime());
            break;
        default:
            out.writeByte(NULL);
            break;
        }
    }
    /*
     * (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(GSColumnKeyWritable o) {
        if (type_ == o.type_) {
            switch (type_) {
            case INTEGER:
                return ((Integer) value_).compareTo(o.getInteger());
            case LONG:
                return ((Long) value_).compareTo(o.getLong());
            case STRING:
                return ((String) value_).compareTo(o.getString());
            case TIMESTAMP:
                return ((Date) value_).compareTo(o.getTimestamp());
            default:
                return 0;
            }
        } else {
            switch (type_) {
            case INTEGER:
                return -1;
            case LONG:
                if (o.type_ == GSType.INTEGER) {
                    return 1;
                } else {
                    return -1;
                }
            case STRING:
                if (o.type_ == GSType.INTEGER || o.type_ == GSType.LONG) {
                    return 1;
                } else {
                    return -1;
                }
            case TIMESTAMP:
                if (o.type_ == GSType.INTEGER || o.type_ == GSType.LONG || o.type_ == GSType.STRING) {
                    return 1;
                } else {
                    return -1;
                }
            default:
                return 1;
            }
        }
    }

    private String readString(DataInput in) throws IOException {
        int len = in.readInt();
        byte[] buffer = new byte[len];
        in.readFully(buffer);
        return new String(buffer, "UTF-8");
    }
    private void writeString(DataOutput out, String obj) throws IOException {
        byte[] buffer = obj.getBytes("UTF-8");
        out.writeInt(buffer.length);
        out.write(buffer);
    }
}
