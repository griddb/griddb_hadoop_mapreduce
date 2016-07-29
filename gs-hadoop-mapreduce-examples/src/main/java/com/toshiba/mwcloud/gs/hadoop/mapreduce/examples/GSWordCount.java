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

package com.toshiba.mwcloud.gs.hadoop.mapreduce.examples;

import static com.toshiba.mwcloud.gs.GSType.INTEGER;
import static com.toshiba.mwcloud.gs.GSType.STRING;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowSet;

import com.toshiba.mwcloud.gs.hadoop.io.GSColumnKeyWritable;
import com.toshiba.mwcloud.gs.hadoop.io.GSRowWritable;
import com.toshiba.mwcloud.gs.hadoop.mapreduce.GSMap;
import com.toshiba.mwcloud.gs.hadoop.mapreduce.GSReduce;
import com.toshiba.mwcloud.gs.hadoop.mapreduce.GSRowInputFormat;
import com.toshiba.mwcloud.gs.hadoop.mapreduce.GSRowOutputFormat;

/**
 * <div lang="ja">
 * GridDB中のデータに対してMapReduce版WordCountを実行します。
 * </div><div lang="en">
 * A MapReduce WordCount program for the data in GridDB.
 * </div>
 */
public class GSWordCount extends Configured implements Tool {
    private static final String APP_NAME = "wordcount";

    /**
     * <div lang="ja">
     * WordCount用Mapクラスです。
     * </div><div lang="en">
     * Map class for WordCount.
     * </div>
     */
    public static class Map extends GSMap<GSColumnKeyWritable, GSRowWritable, Text, IntWritable> {
        private static final IntWritable one_ = new IntWritable(1);
        private final Text word_ = new Text();

        /**
         * <div lang="ja">
         * WordCountのmap処理を実行します。<br/>
         * 取得したロウデータの2番目のカラム(カラム番号が1のカラム)に処理対象の文字列が格納されています。
         * @param key ロウキーが格納されたGSColumnKeyWritableオブジェクト
         * @param value 取得したロウデータが格納されたGSRowWritableオブジェクト
         * @param context Contextオブジェクト
         * @throws IOException
         * @throws InterruptedException
         * </div><div lang="en">
         * Run WordCount map task.<br/>
         * The string is stored in the second column (the column number is 1) of the acquired row data.
         * @param key GSColumnKeyWritable object where the row key is stored
         * @param value GSRowWritable object where the acquired row data is stored
         * @param context Context object
         * @throws IOException
         * @throws InterruptedException
         * </div>
         */
        @Override
        public void map(GSColumnKeyWritable key, GSRowWritable value, Context context)
            throws IOException, InterruptedException {
            String line = (String) value.getValue(1);

            String[] words = line.split("[\t ]");
            for (String word : words) {
                word_.set(word);
                context.write(word_, one_);
            }
        }
    }

    /**
     * <div lang="ja">
     * WordCount用Reduceクラスです。
     * </div><div lang="en">
     * Reduce class for WordCount.
     * </div>
     */
    public static class Reduce extends GSReduce<Text, IntWritable, NullWritable, GSRowWritable> {
        private static final GSType[] rowInfo_ = {STRING, INTEGER};
        private final GSRowWritable row_ = new GSRowWritable(rowInfo_);

        /**
         * <div lang="ja">
         * WordCount用reduce処理を実行します。<br/>
         * 集計した単語毎の出現頻度をGridDBに格納します。<br/>
         * 格納するロウデータの先頭カラム(ロウキー)は単語で、2番目のカラムに出現頻度を整数値で格納します。<br/>
         * そのため、出力先コンテナは二つのカラムを有し、先頭カラムが文字列型で2番目のカラムが整数型でなくてはなりません。
         * @param key 単語
         * @param values 出現頻度のIterable
         * @param context Contextオブジェクト
         * @throws IOException
         * @throws InterruptedException
         * </div><div lang="en">
         * Run reduce task for WordCount.<br/>
         * Store the frequency of occurrence of each word in GridDB.<br/>
         * The output container has two columns, with the lead column being a word and the second column being the frequency.
         * @param key word
         * @param values Iterable frequency of occurrence
         * @param context Context object
         * @throws IOException
         * @throws InterruptedException
         * </div>
         */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            row_.setValue(0, key.toString());
            row_.setValue(1, sum);
            context.write(NullWritable.get(), row_);
        }
    }

    /**
     * <div lang="ja">
     * WordCountのMapReduceジョブを実行します。
     * @param args コマンド引数
     * @return ジョブが正常終了すれば0、そうでなければ1
     * @throws Exception 処理に失敗しました
     * </div><div lang="en">
     * Run a MapReduce job of WordCount.
     * @param args command argument
     * @return 0 for normal termination of the job and 1 otherwise
     * @throws Exception processing failed.
     * </div>
     */
    public int run(String[] args) throws Exception {
        GSConf gsConf = new GSConf();
        gsConf.parseArg(args);

        Configuration conf = getConf();
        gsConf.setup(conf);

        Job job = Job.getInstance(conf, APP_NAME);
        job.setJarByClass(GSWordCount.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(GSRowWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(GSRowInputFormat.class);
        job.setOutputFormatClass(GSRowOutputFormat.class);

        int res = job.waitForCompletion(true) ? 0 : 1;

        if (res == 0) {
            printResult(gsConf);
        }

        return res;
    }

    private void printResult(GSConf gsConf) throws IOException {
        GridStore gridstore = GridStoreFactory.getInstance().getGridStore(gsConf.getGSProperties());
        Container<Object, Row> container = gridstore.getContainer(gsConf.getOutputContainerName());
        Query<Row> query = container.query("select *");
        RowSet<Row> rowSet = query.fetch();
        System.out.println();
        while (rowSet.hasNext()) {
            Row row = rowSet.next();
            String word = row.getString(0);
            int count = row.getInteger(1);
            System.out.printf("%8d\t%s\n", count, word);
        }
        System.out.println();
        rowSet.close();
        query.close();
        container.close();
        gridstore.close();
    }

    /**
     * <div lang="ja">
     * mainプログラム
     * @param args コマンド引数
     * @throws Exception 処理に失敗しました
     * </div><div lang="en">
     * main program
     * @param args command line arguments
     * @throws Exception processing failed
     * </div>
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new GSWordCount(), args);
        System.exit(res);
    }
}
