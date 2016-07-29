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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import com.toshiba.mwcloud.gs.hadoop.conf.GDPropertyUtils;
import com.toshiba.mwcloud.gs.hadoop.mapred.GSContainerSplit;

import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.ContainerType;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.PartitionController;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowKeyPredicate;
import com.toshiba.mwcloud.gs.RowSet;

/**
 * <div lang="ja">
 * GridDB用RecordReaderクラス共通の処理を実行します。
 * </div><div lang="en">
 * Common processing for GridDB RecordReaders.
 * </div>
 */
public class GDRecordReader {
    private Configuration conf_;

    private GridStore gridstore_;
    private PartitionController controller_;

    private String[] partitionHost_;
    private String[][] containerNameList_;

    private String tqlWhere_;

    private int partitionIndex_;
    private int containerIndex_;

    private int containerCount_;
    private int containerPos_;

    private int multiGetSize_;
    private int fetchSize_;

    private List<Container<Object, Row>> containerList_;
    private List<Query<Row>> queryList_;
    private List<GDRowSet> rowsetList_;
    private int rowsetIndex_;

    protected ContainerType containerType_;

    protected boolean keyCheckFlag_ = false;
    protected GSType keyType_ = null;

    /**
     * <div lang="ja">
     * コンストラクタ
     * @param split GSContainerSplitオブジェクト
     * @param conf Configurationオブジェクト
     * @throws GSException GridDBで例外が発生しました
     * </div><div lang="en">
     * Constructor
     * @param split GSContainerSplit object
     * @param conf Configuration object
     * @throws GSException an exception occurred in GridDB
     * </div>
     */
    public GDRecordReader(GSContainerSplit split, Configuration conf) throws GSException {
        conf_ = conf;

        Properties prop = GDPropertyUtils.getGSProp(conf, GDPropertyUtils.getGSPropertiesToRead());
        gridstore_ = GridStoreFactory.getInstance().getGridStore(prop);
        controller_ = gridstore_.getPartitionController();

        partitionHost_ = split.getLocations();
        containerNameList_ = split.getContainerNameList();

        tqlWhere_ = GS_INPUT_TQL_WHERE.get(conf);

        multiGetSize_ = GDPropertyUtils.getInteger(conf, GS_INPUT_FETCH_CONTAINERS);

        fetchSize_ = GDPropertyUtils.getInteger(conf, GS_INPUT_FETCH_SIZE);

        partitionIndex_ = 0;
        containerIndex_ = 0;

        containerCount_ = 0;
        containerPos_ = 0;
        for (int i = 0; i < containerNameList_.length; i++) {
            containerCount_ += containerNameList_[i].length;
        }

        containerList_ = null;
        queryList_ = null;
        rowsetList_ = null;
        rowsetIndex_ = 0;

        proceed();
    }

    /**
     * <div lang="ja">
     * 入力対象コンテナのロウキーの型を返します。
     * @return ロウキーの型
     * </div><div lang="en">
     * Return the row key type of the container for input processing.
     * @return data type of row key
     * </div>
     */
    public GSType getKeyType() {
        return keyType_;
    }

    /**
     * <div lang="ja">
     * 処理済みのコンテナ数を返します。
     * @return 処理済みのコンテナ数
     * </div><div lang="en">
     * Return the number of containers processed.
     * @return number of containers processed
     * </div>
     */
    public long getPos() {
        return containerPos_;
    }

    /**
     * <div lang="ja">
     * 処理済みの割合を計算します。
     * @return 処理済みのコンテナ数の割合
     * </div><div lang="en">
     * Calculate the proportion of containers processed.
     * @return proportion of containers processed
     * </div>
     */
    public float getProgress() {
        if (containerPos_ == 0) {
            return 0.0f;
        } else {
            float progress = ((float) containerPos_) / ((float) containerCount_);
            if (progress > 1.0) {
                progress = 1;
            }
            return progress;
        }
    }

    /**
     * <div lang="ja">
     * クローズ処理を行います。
     * @throws GSException GridDBで例外が発生しました
     * </div><div lang="en">
     * Perform close processing.
     * @throws GSException an exception occurred in GridDB
     * </div>
     */
    public void close() throws GSException {
        closeRowSet();
        if (gridstore_ != null) {
            gridstore_.close();
        }
    }

    /**
     * <div lang="ja">
     * 読み込んだロウデータを管理するGDRowSetオブジェクトを返します。
     * @return GDRowSetオブジェクト
     * @throws GSException GridDBで例外が発生しました
     * </div><div lang="en">
     * Return the GDRowSet object to read row data.
     * @return GDRowSet object
     * @throws GSException an exception occurred in GridDB
     * </div>
     */
    public GDRowSet getRowSet() throws GSException {
        if (! hasNext()) {
            return null;
        }
        GDRowSet rowset = rowsetList_.get(rowsetIndex_);
        return rowset;
    }

    /**
     * <div lang="ja">
     * 取得できるロウデータが残っているかを判定します。
     * @return ロウデータが残っていればtrue、そうでなければfalse
     * @throws GSException GridDBで例外が発生しました
     * </div><div lang="en">
     * Determine whether acquirable row data remains.
     * @return true if row data remains and false otherwise
     * @throws GSException an exception occurred in GridDB
     * </div>
     */
    public boolean hasNext() throws GSException {
        if (rowsetList_ == null) {
            if (! proceed()) {
                return false;
            }
            rowsetIndex_ = 0;
        } else if (rowsetIndex_ >= rowsetList_.size()) {
            if (! proceed()) {
                containerList_ = null;
                queryList_ = null;
                rowsetList_ = null;
                return false;
            }
            rowsetIndex_ = 0;
        }

        GDRowSet rs = rowsetList_.get(rowsetIndex_);
        while (rs != null && (! rs.hasNext())) {
            rowsetList_.get(rowsetIndex_).close();
            Query<Row> query = queryList_.get(rowsetIndex_);
            if (query != null) {
                query.close();
            }
            Container<Object, Row> container = containerList_.get(rowsetIndex_);
            if (container != null) {
                container.close();
            }
            rowsetIndex_++;
            if (rowsetIndex_ >= rowsetList_.size()) {
                if (! proceed()) {
                    containerList_ = null;
                    queryList_ = null;
                    rowsetList_ = null;
                    return false;
                }
                rowsetIndex_ = 0;
            }
            rs = rowsetList_.get(rowsetIndex_);
        }

        if (rs == null) {
            return false;
        }
        return true;
    }

    private void closeRowSet() throws GSException {
        if (rowsetList_ != null) {
            for (int i = rowsetIndex_; i < rowsetList_.size(); i++) {
                rowsetList_.get(i).close();
                Query<Row> query = queryList_.get(i);
                if (query != null) {
                    query.close();
                }
                Container<Object, Row> container = containerList_.get(i);
                if (container != null) {
                    container.close();
                }
            }
            rowsetList_ = null;
            containerList_ = null;
        }
    }

    private boolean proceed() throws GSException {
        closeRowSet();

        if (partitionIndex_ >= containerNameList_.length) {
            return false;
        }

        while (containerIndex_ >= containerNameList_[partitionIndex_].length) {
            partitionIndex_ += 1;

            containerIndex_ = 0;

            if (partitionIndex_ >= containerNameList_.length) {
                return false;
            }
        }

        int numContainers = multiGetSize_;
        if (numContainers == 0) {
            numContainers = containerNameList_[partitionIndex_].length - containerIndex_;
        } else if (numContainers > containerNameList_[partitionIndex_].length - containerIndex_) {
            numContainers = containerNameList_[partitionIndex_].length - containerIndex_;
        }

        createRowSetList(gridstore_, containerNameList_[partitionIndex_], containerIndex_, tqlWhere_, numContainers);

        containerIndex_ += numContainers;
        containerPos_ += numContainers;

        if (containerIndex_ >= containerNameList_[partitionIndex_].length) {
            partitionIndex_ += 1;
            containerPos_ += 1;

            containerIndex_ = 0;
        }

        return true;
    }

    private void createRowSetList(GridStore gridstore, String[] containerNameList, int n,
            String cond, int numContainers) throws GSException {
        if (containerIndex_ == 0) {
            try {
                controller_.assignPreferableHost(partitionIndex_, InetAddress.getByName(partitionHost_[partitionIndex_]));
            } catch (UnknownHostException e) {
                throw new GSException(e);
            }
        }

        containerList_ = new ArrayList<Container<Object, Row>>(numContainers);
        queryList_ = new ArrayList<Query<Row>>(numContainers);
        rowsetList_ = new ArrayList<GDRowSet>(numContainers);

        ContainerInfo containerInfo = gridstore.getContainerInfo(containerNameList[n]);
        if (containerInfo == null) {
            throw new GSException("Unknown container name(" + containerNameList[n] + ")");
        }
        for (int i = 1; i < numContainers; i++) {
            if (gridstore.getContainerInfo(containerNameList[n + i]) == null) {
                throw new GSException("Unknown container name(" + containerNameList[n + i] + ")");
            }
        }

        if (! keyCheckFlag_) {
            keyCheckFlag_ = true;
            if (containerInfo.isRowKeyAssigned()) {
                ColumnInfo columnInfo = containerInfo.getColumnInfo(0);
                keyType_ = columnInfo.getType();
            }

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
            conf_.set(GS_INPUT_ROW_KEY_ASSIGNED.getKey(), Boolean.valueOf(containerInfo.isRowKeyAssigned()).toString());
            conf_.set(GS_INPUT_COLUMN_TYPE_LIST.getKey(), columnTypeBuffer.toString());
            conf_.set(GS_INPUT_COLUMN_NAME_LIST.getKey(), columnNameBuffer.toString());
        }

        boolean isMultiGet = false;
        if (cond == null && fetchSize_ == 0) {
            if (keyType_ != null) {
                switch (keyType_) {
                case STRING:
                case INTEGER:
                case LONG:
                case TIMESTAMP:
                    isMultiGet = true;
                    break;
                default:
                    break;
                }
            }
        }

        if (isMultiGet) {
            Map<String, List<Row>> resultMap = null;
            switch (keyType_) {
            case STRING:
                Map<String, RowKeyPredicate<Object>> containerPredicateMapString = new HashMap<String, RowKeyPredicate<Object>>();
                RowKeyPredicate<Object> rowKeyPredicateString = RowKeyPredicate.create(GSType.STRING);
                for (int i = 0; i < numContainers; i++) {
                    containerPredicateMapString.put(containerNameList[n + i], rowKeyPredicateString);
                }
                resultMap = gridstore.multiGet(containerPredicateMapString);
                break;
            case INTEGER:
                Map<String, RowKeyPredicate<Object>> containerPredicateMapInteger = new HashMap<String, RowKeyPredicate<Object>>();
                RowKeyPredicate<Object> rowKeyPredicateInteger = RowKeyPredicate.create(GSType.INTEGER);
                for (int i = 0; i < numContainers; i++) {
                    containerPredicateMapInteger.put(containerNameList[n + i], rowKeyPredicateInteger);
                }
                resultMap = gridstore.multiGet(containerPredicateMapInteger);
                break;
            case LONG:
                Map<String, RowKeyPredicate<Object>> containerPredicateMapLong = new HashMap<String, RowKeyPredicate<Object>>();
                RowKeyPredicate<Object> rowKeyPredicateLong = RowKeyPredicate.create(GSType.LONG);
                for (int i = 0; i < numContainers; i++) {
                    containerPredicateMapLong.put(containerNameList[n + i], rowKeyPredicateLong);
                }
                resultMap = gridstore.multiGet(containerPredicateMapLong);
                break;
            case TIMESTAMP:
                Map<String, RowKeyPredicate<Object>> containerPredicateMapDate = new HashMap<String, RowKeyPredicate<Object>>();
                RowKeyPredicate<Object> rowKeyPredicateDate = RowKeyPredicate.create(GSType.TIMESTAMP);
                for (int i = 0; i < numContainers; i++) {
                    containerPredicateMapDate.put(containerNameList[n + i], rowKeyPredicateDate);
                }
                resultMap = gridstore.multiGet(containerPredicateMapDate);
                break;
            default:
                break;
            }
            for (int i = 0; i < numContainers; i++) {
                List<Row> rowList = resultMap.get(containerNameList[n + i]);
                containerList_.add(null);
                queryList_.add(null);
                rowsetList_.add(new GDRowSet(rowList));
            }
        } else {
            execQuery(gridstore, containerNameList, n, cond, numContainers);
        }
    }

    private void execQuery(GridStore gridstore, String[] containerNameList, int n,
            String cond, int numContainers) throws GSException {
        if (fetchSize_ > 0) {
            multiGetSize_ = 1;
        }
        String tql = createTql(cond, fetchSize_);

        if (multiGetSize_ == 0 || multiGetSize_ > 1) {
            for (int i = 0; i < numContainers; i++) {
                Container<Object, Row> container = getContainer(gridstore, containerNameList[n + i]);
                containerList_.add(container);

                Query<Row> query = container.query(tql);
                queryList_.add(query);
            }
            gridstore.fetchAll(queryList_);
            for (int i = 0; i < numContainers; i++) {
                Query<Row> query = queryList_.get(i);
                RowSet<Row> rowset = query.getRowSet();
                rowsetList_.add(new GDRowSet(query, rowset, null, tql, 0));
            }
        } else {
            for (int i = 0; i < numContainers; i++) {
                Container<Object, Row> container = getContainer(gridstore, containerNameList[n + i]);
                containerList_.add(container);

                Query<Row> query = container.query(tql);
                queryList_.add(query);

                RowSet<Row> rowset = null;
                rowset = query.fetch(false);
                rowsetList_.add(new GDRowSet(query, rowset, containerList_.get(i), tql, fetchSize_));
            }
        }
    }

    private String createTql(String cond, int tqlLimit) {
        String tql = "select *";
        if (cond != null) {
            cond = cond.trim();
            if (cond.length() == 0) {
                cond = null;
            }
        }
        if (cond != null) {
            tql += " where " + cond;
        }
        if (tqlLimit > 0) {
            tql += " limit " + tqlLimit;
        }
        return tql;
    }

    private Container<Object, Row> getContainer(GridStore gridstore, String containerName)
            throws GSException {
        Container<Object, Row> container = gridstore.getContainer(containerName);

        if (container == null) {
            throw new GSException("UNKNOWN CONTAINER(" + containerName + ")");
        }

        return container;
    }
}
