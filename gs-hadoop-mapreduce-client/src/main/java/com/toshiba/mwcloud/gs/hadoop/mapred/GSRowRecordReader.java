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

package com.toshiba.mwcloud.gs.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.Row;

import com.toshiba.mwcloud.gs.hadoop.io.GSColumnKeyWritable;
import com.toshiba.mwcloud.gs.hadoop.io.GSRowWritable;
import com.toshiba.mwcloud.gs.hadoop.util.GDRecordReader;
import com.toshiba.mwcloud.gs.hadoop.util.GDRowSet;

/**
 * <div lang="ja">
 * GridDBのRowオブジェクトを利用したGridDB用RecordReaderクラスです。
 * </div><div lang="en">
 * GridDB RecordReader class using GridDB Row object.
 * </div>
 */
public class GSRowRecordReader implements RecordReader<GSColumnKeyWritable, GSRowWritable> {
    private GDRecordReader reader_;

    /**
     * <div lang="ja">
     * コンストラクタ
     * @param conf　Configurationオブジェクト
     * @param split　GSContainerSplitオブジェクト
     * @throws GSException　GridDBで例外が発生しました
     * </div><div lang="en">
     * Constructor
     * @param conf Configuration object
     * @param split GSContainerSplit object
     * @throws GSException an exception occurred in GridDB
     * </div>
     */
    public GSRowRecordReader(JobConf conf, GSContainerSplit split) throws IOException {
        reader_ = new GDRecordReader(split, conf);
    }

    /*
     * (non Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#getPos()
     */
    @Override
    public long getPos() throws IOException {
        return reader_.getPos();
    }

    /*
     * (non Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#getProgress()
     */
    @Override
    public float getProgress() throws IOException {
        return reader_.getProgress();
    }

    /*
     * (non Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#close()
     */
    @Override
    public void close() throws IOException {
        reader_.close();
    }

    /**
     * <div lang="ja">
     * GridDBから次のRowオブジェクトを取得します。
     * @param key 取得したロウキーの値を格納するためのGSColumnKeyWritableオブジェクト
     * @param value 取得したRowオブジェクト中の値を格納するためのGSRowWritableオブジェクト
     * @return Rowオブジェクトを取得できればtrue、取得できるデータが残っていなければfalse
     * @throws IOException Rowオブジェクトの取得でエラーが発生しました
     * </div><div lang="en">
     * Get next Row object from GridDB.
     * @param key GSColumnKeyWritable object to store value of acquired row key
     * @param value GSRowWritable object to store value in acquired Row object
     * @return true if Row object can be acquired and false if acquirable data remains
     * @throws IOException an error occurred in acquiring Row object
     * </div>
     */
    @Override
    public boolean next(GSColumnKeyWritable key, GSRowWritable value) throws IOException {
        if (! reader_.hasNext()) {
            return false;
        }

        GDRowSet rowSet = reader_.getRowSet();
        Row row = rowSet.nextRow();
        value.setValues(row);

        GSType type = key.getType();
        if (type != null) {
            switch (type) {
            case INTEGER:
                key.setInteger(row.getInteger(0));
                break;
            case LONG:
                key.setLong(row.getLong(0));
                break;
            case STRING:
                key.setString(row.getString(0));
                break;
            case TIMESTAMP:
                key.setTimestamp(row.getTimestamp(0));
                break;
            default:
                throw new IOException("GSColumnKeyWritable: Illegal type(" + type + ")");
            }
        }

        return true;
    }

    /*
     * (non Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#createKey()
     */
    @Override
    public GSColumnKeyWritable createKey() {
        return new GSColumnKeyWritable(reader_.getKeyType());
    }

    /*
     * (non Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#createValue()
     */
    @Override
    public GSRowWritable createValue() {
        return new GSRowWritable();
    }
}
