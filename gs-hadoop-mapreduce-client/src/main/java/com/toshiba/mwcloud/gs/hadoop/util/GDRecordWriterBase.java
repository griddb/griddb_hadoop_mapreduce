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

package com.toshiba.mwcloud.gs.hadoop.util;

import static com.toshiba.mwcloud.gs.hadoop.conf.GDProperty.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.toshiba.mwcloud.gs.hadoop.conf.GDPropertyUtils;

import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Row;

/**
 * <div lang="ja">
 * GridDB用RecordWriterクラス共通処理の基底クラスです。
 * </div><div lang="en">
 * Base class of common processing for GridDB RecordWriters.
 * </div>
 */
public class GDRecordWriterBase {
    protected TaskAttemptContext context_;

    protected Configuration conf_;

    protected GridStore gridstore_;

    protected int rowBufferSize_;
    protected Row[] rowBuffer_;
    protected int rowBufferCount_;

    /**
     * <div lang="ja">
     * コンストラクタ
     * @param context TaskAttemptContextオブジェクト
     * @throws GSException GridDBで例外が発生しました
     * </div><div lang="en">
     * Constructor
     * @param context TaskAttemptContext object
     * @throws GSException an exception occurred in GridDB
     * </div>
     */
    public GDRecordWriterBase(TaskAttemptContext context) throws GSException {
        context_ = context;
        Configuration conf = context.getConfiguration();

        conf_ = conf;

        Properties prop = GDPropertyUtils.getGSProp(conf, GDPropertyUtils.getGSPropertiesToWrite());
        gridstore_ = GridStoreFactory.getInstance().getGridStore(prop);

        rowBufferSize_ = GDPropertyUtils.getInteger(conf, GS_OUTPUT_ROW_BUFFER_SIZE);

        rowBuffer_ = null;
        rowBufferCount_ = 0;
    }

    protected void initRowBuffer(ContainerInfo containerInfo) throws GSException {
        if (rowBuffer_ == null) {
            rowBuffer_ = new Row[rowBufferSize_];
            for (int i = 0; i < rowBufferSize_; i++) {
                rowBuffer_[i] = gridstore_.createRow(containerInfo);
            }
            rowBufferCount_ = 0;

            List<String> columnTypeList = new ArrayList<String>();
            List<String> columnNameList = new ArrayList<String>();
            for (int i = 0; i < containerInfo.getColumnCount(); i++) {
                ColumnInfo columnInfo = containerInfo.getColumnInfo(i);
                columnTypeList.add(columnInfo.getType().toString());
                columnNameList.add(columnInfo.getName());
            }
            StringBuffer columnTypeBuffer = new StringBuffer(columnTypeList.get(0));
            for (int i = 1; i < columnTypeList.size(); i++) {
                columnTypeBuffer.append("," + columnTypeList.get(i));
            }
            StringBuffer columnNameBuffer = new StringBuffer(columnNameList.get(0));
            for (int i = 1; i < columnNameList.size(); i++) {
                columnNameBuffer.append("," + columnNameList.get(i));
            }
            conf_.set(GS_OUTPUT_ROW_KEY_ASSIGNED.getKey(), Boolean.valueOf(containerInfo.isRowKeyAssigned()).toString());
            conf_.set(GS_OUTPUT_COLUMN_TYPE_LIST.getKey(), columnTypeBuffer.toString());
            conf_.set(GS_OUTPUT_COLUMN_NAME_LIST.getKey(), columnNameBuffer.toString());
        }
    }
}
