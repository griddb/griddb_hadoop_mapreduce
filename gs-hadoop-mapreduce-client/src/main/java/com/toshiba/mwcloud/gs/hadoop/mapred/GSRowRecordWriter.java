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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import com.toshiba.mwcloud.gs.Row;

import com.toshiba.mwcloud.gs.hadoop.io.GSRowWritable;
import com.toshiba.mwcloud.gs.hadoop.util.GDRecordWriter;

/**
 * <div lang="ja">
 * GridDBのRowオブジェクトを利用したGridDB用RecordWriterクラスです。
 * </div><div lang="en">
 * GridDB RecordWriter class using GridDB Row object.
 * </div>
 */
public class GSRowRecordWriter implements RecordWriter<NullWritable, GSRowWritable> {
    private GDRecordWriter writer_;

    /**
     * <div lang="ja">
     * コンストラクタ
     * @param conf　Configurationオブジェクト
     * @throws IOException　GridDBで例外が発生しました
     * </div><div lang="en">
     * Constructor
     * @param conf Configuration object
     * @throws IOException an exception occurred in GridDB
     * </div>
     */
    public GSRowRecordWriter(JobConf conf) throws IOException {
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, TaskAttemptID.forName(conf.get("mapred.task.id")));
        writer_ = new GDRecordWriter(context);
    }

    /*
     * (non Javadoc)
     * @see org.apache.hadoop.mapred.RecordWriter#close(org.apache.hadoop.mapred.Reporter)
     */
    @Override
    public void close(Reporter reporter) throws IOException {
        writer_.close();
    }

    /**
     * <div lang="ja">
     * データをGridDBに書き込みます。
     * @param key NullWritableオブジェクト
     * @param value 書き込み対象のデータが設定されたGSRowWritableオブジェクト
     * @throws IOException 書き込み処理でエラーが発生しました
     * </div><div lang="en">
     * Write data to GridDB.
     * @param key NullWritable object
     * @param value GSRowWritable object in which write data is set
     * @throws IOException an error occurred during writing process
     * </div>
     */
    @Override
    public void write(NullWritable key, GSRowWritable value) throws IOException {
        Row row = writer_.getRow();
        value.getValues(row);
        writer_.putRow(row);
    }
}
