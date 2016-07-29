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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Mapper;

/**
 * <div lang="ja">
 * GridDB用Mapインタフェースです。
 * @param <KEYIN> Mapの入力キーの型
 * @param <VALIN> Mapの入力値の型
 * @param <KEYOUT> Mapの出力キーの型
 * @param <VALOUT> Mapの出力値の型
 * </div><div lang="en">
 * GridDB Map interface.
 * @param <KEYIN> Data type of Map input key
 * @param <VALIN> Data type of Map input value
 * @param <KEYOUT> Data type of Map output key
 * @param <VALOUT> Data type of Map output value
 * </div>
 */
public interface GSMap<KEYIN extends Writable, VALIN extends Writable, KEYOUT extends WritableComparable<?>, VALOUT extends Writable>
        extends Mapper<KEYIN, VALIN, KEYOUT, VALOUT> {
}
