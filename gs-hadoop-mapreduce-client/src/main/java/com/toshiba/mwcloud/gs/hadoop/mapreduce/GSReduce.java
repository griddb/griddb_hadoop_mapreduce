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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <div lang="ja">
 * GridDB用Reduceクラスです。
 * @param <KEYIN> Reduceの入力キーの型
 * @param <VALIN> Reduceの入力値の型
 * @param <KEYOUT> Reduceの出力キーの型
 * @param <VALOUT> Reduceの出力値の型
 * </div><div lang="en">
 * GridDB Reduce class.
 * @param <KEYIN> Data type of Reduce input key
 * @param <VALIN> Data type of Reduce input value
 * @param <KEYOUT> Data type of Reduce output key
 * @param <VALOUT> Data type of Reduce output value
 * </div>
 */
public abstract class GSReduce<KEYIN extends WritableComparable<? super KEYIN>, VALIN extends Writable, KEYOUT extends Writable, VALOUT extends Writable>
        extends Reducer<KEYIN, VALIN, KEYOUT, VALOUT> {
}
