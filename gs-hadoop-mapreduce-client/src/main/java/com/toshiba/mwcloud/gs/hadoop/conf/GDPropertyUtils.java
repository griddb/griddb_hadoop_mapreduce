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

package com.toshiba.mwcloud.gs.hadoop.conf;

import static com.toshiba.mwcloud.gs.hadoop.conf.GDProperty.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

/**
 * <div lang="ja">
 * MapReduceジョブ設定に関するユーティリティです。
 * </div><div lang="en">
 * Utilities of MapReduce job settings.
 * </div>
 */
public class GDPropertyUtils {
    /**
     * <div lang="ja">
     * GridDB接続用GDProperty型定数一覧（入力処理用）
     * </div><div lang="en">
     * List of GDProperty objects for GridDB connection (for input processing)
     * </div>
     */
    private static final GDProperty[] gsPropertiesToRead = {
        GS_INPUT_HOST, GS_INPUT_PORT, GS_INPUT_NOTIFICATION_ADDRESS, GS_INPUT_NOTIFICATION_PORT,
        GS_INPUT_CLUSTER_NAME, GS_INPUT_USER, GS_INPUT_PASSWORD, GS_INPUT_CONSISTENCY,
        GS_INPUT_TRANSACTION_TIMEOUT, GS_INPUT_FAILOVER_TIMEOUT, GS_INPUT_CONTAINER_CACHE_SIZE
    };

    /**
     * <div lang="ja">
     * GridDB接続用GDProperty型定数一覧（出力処理用）
     * </div><div lang="en">
     * List of GDProperty objects for GridDB connection (for output processing)
     * </div>
     */
    private static final GDProperty[] gsPropertiesToWrite = {
        GS_OUTPUT_HOST, GS_OUTPUT_PORT, GS_OUTPUT_NOTIFICATION_ADDRESS, GS_OUTPUT_NOTIFICATION_PORT,
        GS_OUTPUT_CLUSTER_NAME, GS_OUTPUT_USER, GS_OUTPUT_PASSWORD, GS_OUTPUT_CONSISTENCY,
        GS_OUTPUT_TRANSACTION_TIMEOUT, GS_OUTPUT_FAILOVER_TIMEOUT, GS_OUTPUT_CONTAINER_CACHE_SIZE
    };

    private GDPropertyUtils() {
    }

    /**
     * <div lang="ja">
     * プロパティ値を区切り文字(',')で分割して値のリストを返します。<br/>
     * 各値の先頭および末尾の空白文字は除去されます。値がない場合は空のリストを返します。
     * @param conf Configurationオブジェクト
     * @param property GridDBコネクタ用プロパティに対応したGDProperty型定数
     * @return 値のリスト
     * @throws GDPropertyException 必須プロパティに値が設定されていないか、プロパティの値が適切でありませんでした
     * </div><div lang="en">
     * Return a list of the property values divided by the comma character (',').<br/>
     * Any leading and trailing whitespace of each value is removed. Return an empty list if there are no values.
     * @param conf Configuration object
     * @param property GDProperty object corresponding to the GridDB connector property
     * @return list of values
     * @throws GDPropertyException either the value cannot be acquired or the value is not appropriate
     * </div>
     */
    public static List<String> getPropertyList(Configuration conf, GDProperty property) {
        String value = property.getProperty(conf);
        String[] frags = value.split(",");
        List<String> list = new ArrayList<String>();
        for (String frag : frags) {
            String v = frag.trim();
            if (v.length() > 0) {
                list.add(v);
            }
        }

        return list;
    }

    /**
     * <div lang="ja">
     * 入力処理対象のGridDBを接続するための列挙型定数一覧を返します。
     * @return GDProperty型定数の配列
     * </div><div lang="en">
     * Return an array of GDProperty objects to connect GridDB for input processing.
     * @return array of GDProperty objects
     * </div>
     */
    public static GDProperty[] getGSPropertiesToRead() {
        return gsPropertiesToRead;
    }

    /**
     * <div lang="ja">
     * 出力処理対象のGridDBを接続するための列挙型定数一覧を返します。
     * @return GDProperty型定数の配列
     * </div><div lang="en">
     * Return an array of GDProperty objects to connect GridDB for output processing.
     * @return array of GDProperty objects
     * </div>
     */
    public static GDProperty[] getGSPropertiesToWrite() {
        return gsPropertiesToWrite;
    }

    /**
     * <div lang="ja">
     * ConfigurationオブジェクトからGridDB接続に必要なPropertiesオブジェクトを生成して返します。
     * @param conf Configurationオブジェクト
     * @param gsProperties GDProperty型定数の配列
     * @return Propertiesオブジェクト
     * @throws GDPropertyException 必須プロパティに値が設定されていないか、プロパティの値が適切でありませんでした
     * </div><div lang="en">
     * Return the Properties object to connect GridDB.
     * @param conf Configuration object
     * @param gsProperties array of GDProperty objects
     * @return Properties object
     * @throws GDPropertyException either the value cannot be acquired or the value is not appropriate
     * </div>
     */
    public static Properties getGSProp(Configuration conf, GDProperty[] gsProperties) {
        Properties prop = new Properties();
        for (GDProperty property : gsProperties) {
            String key = property.getGSPropertyKey();
            if (key != null) {
                String value = property.getProperty(conf);
                if (value.trim().length() > 0) {
                    prop.put(key, value);
                }
            }
        }

        return prop;
    }

    /**
     * <div lang="ja">
     * プロパティに設定された整数値を返します。
     * @param conf Configurationオブジェクト
     * @param property GDProperty型定数
     * @return プロパティの整数値
     * @throws NumberFormatException
     * </div><div lang="en">
     * Return the integer value set in the property.
     * @param conf Configuration object
     * @param property GDProperty object
     * @return integer value of property
     * @throws NumberFormatException
     * </div>
     */
    public static int getInteger(Configuration conf, GDProperty property) throws NumberFormatException {
        return Integer.valueOf(property.getProperty(conf));
    }
}
