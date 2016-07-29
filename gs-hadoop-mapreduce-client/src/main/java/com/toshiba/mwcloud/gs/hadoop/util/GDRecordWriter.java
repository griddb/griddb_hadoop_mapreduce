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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.Row;

import java.util.ArrayList;

/**
 * <div lang="ja">
 * GridDB用RecordWriterクラス共通の処理を実行します。
 * </div><div lang="en">
 * Common processing for GridDB RecordWriters.
 * </div>
 */
public class GDRecordWriter extends GDRecordWriterBase {
    private String containerName_;

    private List<Row> listRow_ = null;

    /**
     * <div lang="ja">
     * コンストラクタ
     * @param context TaskAttempContextオブジェクト
     * @throws GSException GridDBで例外が発生しました
     * </div><div lang="en">
     * Constructor
     * @param context TaskAttempContext object
     * @throws GSException the output container name has not been specified or an exception occurred in GridDB
     * </div>
     */
    public GDRecordWriter(TaskAttemptContext context) throws GSException {
        super(context);

        String containerName = GS_OUTPUT_CONTAINER_NAME.get(conf_);

        if (containerName == null) {
            throw new GSException("'" + GS_OUTPUT_CONTAINER_NAME.getKey() + "' is not defined");
        }

        initialize(conf_, containerName);
    }

    /**
     * <div lang="ja">
     * Rowオブジェクトを書き込み用バッファに格納します。<br/>
     * バッファが溢れたらバッファ中のRowオブジェクトを一括してGridDBに書き込みます。
     * @param row Rowオブジェクト
     * @throws GSException GridDBで例外が発生しました
     * </div><div lang="en">
     * Store Row objects in write buffer and write Row objects in the buffer when overflowed to GridDB.
     * @param row Row object
     * @throws GSException an exception occurred in GridDB
     * </div>
     */
    public void putRow(Row row) throws GSException {
        if (listRow_ == null) {
            listRow_ = new ArrayList<Row>();
        }
        listRow_.add(row);

        rowBufferCount_++;
        if (rowBufferCount_ >= rowBufferSize_) {
            flushBuffer();
        }
    }

    /**
     * <div lang="ja">
     * 書き込み用データを設定するための空のRowオブジェクトを返します。
     * @return Rowオブジェクト
     * @throws GSException　GridDBで例外が発生しました
     * </div><div lang="en">
     * Return the empty Row object for storing write data.
     * @return Row object
     * @throws GSException an exception occurred in GridDB
     * </div>
     */
    public Row getRow() throws GSException {
        return rowBuffer_[rowBufferCount_];
    }

    /**
     * <div lang="ja">
     * クローズ処理を実行します。
     * @throws GSException GridDBで例外が発生しました
     * </div><div lang="en">
     * Perform close processing.
     * @throws GSException an exception occurred in GridDB
     * </div>
     */
    public void close() throws GSException {
        if (rowBufferCount_ > 0) {
            flushBuffer();
        }

        if (gridstore_ != null) {
            gridstore_.close();
            gridstore_ = null;
        }
    }

    private void initialize(Configuration conf, String containerName) throws GSException {
        containerName_ = containerName;
        ContainerInfo containerInfo = gridstore_.getContainerInfo(containerName_);
        if (containerInfo == null) {
            throw new GSException("Unknown container name(" + containerName_ + ")");
        }
        initRowBuffer(containerInfo);
    }

    private void flushBuffer() throws GSException {
        Map<String, List<Row>> rowsMap = new HashMap<String, List<Row>>();
        rowsMap.put(containerName_, listRow_);
        gridstore_.multiPut(rowsMap);
        listRow_.clear();

        rowBufferCount_ = 0;
    }
}
