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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import com.toshiba.mwcloud.gs.hadoop.io.GSRowWritable;

/**
 * <div lang="ja">
 * GridDBのRowオブジェクトを利用したGridDB用OutputFormatクラスです。
 * </div><div lang="en">
 * GridDB OutputFormat class using GridDB Row object.
 * </div>
 */
public class GSRowOutputFormat implements OutputFormat<NullWritable, GSRowWritable> {
    /*
     * (non Javadoc)
     * @see org.apache.hadoop.mapred.OutputFormat#getRecordWriter(org.apache.hadoop.fs.FileSystem, org.apache.hadoop.mapred.JobConf, java.lang.String,
     * org.apache.hadoop.util.Progressable)
     */
    @Override
    public RecordWriter<NullWritable, GSRowWritable> getRecordWriter(FileSystem ignored,
            JobConf job, String name, Progressable progress) throws IOException {
        RecordWriter<NullWritable, GSRowWritable> writer = new GSRowRecordWriter(job);
        return writer;
    }
    /*
     * (non Javadoc)
     * @see org.apache.hadoop.mapred.OutputFormat#checkOutputSpecs(org.apache.hadoop.fs.FileSystem, org.apache.hadoop.mapred.JobConf)
     */
    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    }
}
