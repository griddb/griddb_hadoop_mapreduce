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

package com.toshiba.mwcloud.gs.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.toshiba.mwcloud.gs.hadoop.io.GSColumnKeyWritable;
import com.toshiba.mwcloud.gs.hadoop.io.GSRowWritable;
import com.toshiba.mwcloud.gs.hadoop.util.GDRecordReader;
import com.toshiba.mwcloud.gs.hadoop.util.GDRowSet;

import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.Row;

/**
 * <div lang="ja">
 * GridDBのRowオブジェクトを利用したGridDB用RecordReaderクラスです。
 * </div><div lang="en">
 * GridDB RecordReader class using GridDB Row object.
 * </div>
 */
public class GSRowRecordReader extends RecordReader<GSColumnKeyWritable, GSRowWritable> {
    private GDRecordReader reader_;

    private GSColumnKeyWritable key_;
    private GSRowWritable value_;

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        reader_ = new GDRecordReader(((GSContainerSplit) split).getDelegate(), conf);

        key_ = new GSColumnKeyWritable(reader_.getKeyType());
        value_ = new GSRowWritable();
    }

    /**
     * <div lang="ja">
     * 次のデータを取得できるかを判定します。
     * @return 取得できるデータが残っていればtrue、残っていなければfalse
     * @throws IOException 判定処理でエラーが発生しました
     * </div><div lang="en">
     * Determine whether the next data can be acquired.
     * @return true if acquirable data remains and false otherwise
     * @throws IOException an error occurred in the judgment process
     * </div>
     */
    @Override
    public boolean nextKeyValue() throws IOException {
        if (! reader_.hasNext()) {
            return false;
        }

        GDRowSet rowSet = reader_.getRowSet();
        Row row = rowSet.nextRow();
        value_.setValues(row);

        GSType type = key_.getType();
        if (type != null) {
            switch (type) {
            case INTEGER:
                key_.setInteger(row.getInteger(0));
                break;
            case LONG:
                key_.setLong(row.getLong(0));
                break;
            case STRING:
                key_.setString(row.getString(0));
                break;
            case TIMESTAMP:
                key_.setTimestamp(row.getTimestamp(0));
                break;
            default:
                throw new IOException("GSColumnKeyWritable: Illegal type(" + type + ")");
            }
        }

        return true;
    }

    /**
     * <div lang="ja">
     * 次のロウキーの値を返します。
     * @return ロウキーの値が設定されたGSColumnKeyWritableオブジェクト
     * </div><div lang="en">
     * Return the value of the next row key.
     * @return GSColumnKeyWritable object in which value of row key is set
     * </div>
     */
    @Override
    public GSColumnKeyWritable getCurrentKey() {
        return key_;
    }

    /**
     * <div lang="ja">
     * 次のデータを返します。
     * @return 取得したデータが設定されたGSRowWritableオブジェクト
     * </div><div lang="en">
     * Return the next data.
     * @return GSRowWritable object in which acquired data is set
     * </div>
     */
    @Override
    public GSRowWritable getCurrentValue() {
        return value_;
    }

    /*
     * (non Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#close()
     */
    @Override
    public void close() throws IOException {
        if (reader_ != null) {
            reader_.close();
        }
    }

    /*
     * (non Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
     */
    @Override
    public float getProgress() {
        if (reader_ != null) {
            return reader_.getProgress();
        }
        return 0;
    }
}
