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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.toshiba.mwcloud.gs.hadoop.io.GSRowWritable;

/**
 * <div lang="ja">
 * GridDBのRowオブジェクトを利用したGridDB用OutputFormatクラスです。
 * </div><div lang="en">
 * GridDB OutputFormat class using GridDB Row object.
 * </div>
 */
public class GSRowOutputFormat extends OutputFormat<NullWritable, GSRowWritable> {
    /*
     * (non Javadoc)
     * @see org.apache.hadoop.mapreduce.OutputFormat#getRecordWriter(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    public RecordWriter<NullWritable, GSRowWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        RecordWriter<NullWritable, GSRowWritable> writer = new GSRowRecordWriter(context);
        return writer;
    }
    /*
     * (non Javadoc)
     * @see org.apache.hadoop.mapreduce.OutputFormat#checkOutputSpecs(org.apache.hadoop.mapreduce.JobContext)
     */
    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    }
    /*
     * (non Javadoc)
     * @see org.apache.hadoop.mapreduce.OutputFormat#getOutputCommitter(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new GSOutputCommitter();
    }
}
