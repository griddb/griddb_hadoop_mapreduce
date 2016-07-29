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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.hadoop.io.GSColumnKeyWritable;
import com.toshiba.mwcloud.gs.hadoop.io.GSRowWritable;
import com.toshiba.mwcloud.gs.hadoop.util.GDInputFormat;

/**
 * <div lang="ja">
 * GridDBのRowオブジェクトを利用したGridDB用InputFormatクラスです。
 * </div><div lang="en">
 * GridDB InputFormat class using GridDB Row object.
 * </div>
 */
public class GSRowInputFormat extends InputFormat<GSColumnKeyWritable, GSRowWritable> {
    /**
     * <div lang="ja">
     * GridDB用にInputSplitオブジェクトのリストを生成して返します。<br/>
     * InputSplitの個数は入力対象となるパーティション数とプロパティmapreduce.job.mapsの値 の小さい方の値になります。
     * @param context JobContextオブジェクト
     * @throws GSException GridDBで例外が発生しました
     * </div><div lang="en">
     * Generate a list of GridDB InputSplit objects.<br/>
     * The number of InputSplits will be the smaller of the number of partitions for input processing and the value of property mapreduce.job.maps.
     * @param context JobContext object
     * @throws GSException an exception occurred in GridDB
     * </div>
     */
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        int numSplits = conf.getInt("mapreduce.job.maps", 1);
        GDInputFormat inputFormat = new GDInputFormat();
        List<InputSplit> splits = inputFormat.getSplitList(numSplits, conf);
        return splits;
    }

    /*
     * (non Javadoc)
     * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit,
     * org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public RecordReader<GSColumnKeyWritable, GSRowWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        RecordReader<GSColumnKeyWritable, GSRowWritable> reader = new GSRowRecordReader();
        reader.initialize(split, context);
        return reader;
    }
}
