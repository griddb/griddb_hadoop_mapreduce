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

import org.apache.hadoop.mapred.InputFormat;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

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
public class GSRowInputFormat implements InputFormat<GSColumnKeyWritable, GSRowWritable> {
    /**
     * <div lang="ja">
     * GridDB用のInputSplitオブジェクトの配列を生成します。<br/>
     * InputSplitの個数は入力対象となるパーティション数と引数numSplitsの値の小さい方になります。
     * @param job JobConfオブジェクト
     * @param numSplits スプリット数
     * @throws GSException GridDBで例外が発生しました
     * </div><div lang="en">
     * Generate an array of GridDB InputSplit objects.<br/>
     * The number of InputSplits will be the smaller of the number of partitions for input processing and the value of the argument numSplits.
     * @param job JobConf object
     * @param numSplits number of splits
     * @throws GSException an exception occurred in GridDB
     * </div>
     */
    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws GSException {
        GDInputFormat inputFormat = new GDInputFormat();
        return inputFormat.getSplitArray(numSplits, job);
    }
    /*
     * (non Javadoc)
     * @see org.apache.hadoop.mapred.InputFormat#getRecordReader(org.apache.hadoop.mapred.InputSplit, org.apache.hadoop.mapred.JobConf,
     * org.apache.hadoop.mapred.Reporter)
     */
    @Override
    public RecordReader<GSColumnKeyWritable, GSRowWritable> getRecordReader(InputSplit split,
            JobConf job, Reporter reporter) throws IOException {
        GSContainerSplit gsSplit = (GSContainerSplit) split;
        RecordReader<GSColumnKeyWritable, GSRowWritable> reader = new GSRowRecordReader(job, gsSplit);
        return reader;
    }
}
