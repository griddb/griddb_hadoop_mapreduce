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

import java.util.List;

import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowSet;

/**
 * <div lang="ja">
 * 読み込んだデータを管理します。
 * </div><div lang="en">
 * An iterator of row data for GridDB RecordReaders.
 * </div>
 */
public class GDRowSet {
    private Query<Row> query_;
    private RowSet<Row> rowSet_;

    private List<Row> rowList_;
    private int rowListIndex_;
    private Container<Object, Row> container_;
    private String tql_;
    private int offset_;
    private int limit_;
    private int rowCount_;

    /**
     * <div lang="ja">
     * TQL文で読み込んだデータを管理するためのオブジェクトを生成します。
     * @param query Queryオブジェクト
     * @param rowSet RowSetオブジェクト
     * @param container Containerオブジェクト
     * @param tql TQLのクエリ
     * @param tqlLimit TQLのLIMIT句の値
     * </div><div lang="en">
     * Create an object to read row data using TQL.
     * @param query Query object
     * @param rowSet RowSet object
     * @param container Container object
     * @param tql TQL statement
     * @param tqlLimit value for TQL LIMIT clause
     * </div>
     */
    public GDRowSet(Query<Row> query, RowSet<Row> rowSet, Container<Object, Row> container, String tql, int tqlLimit) {
        query_ = query;
        rowSet_ = rowSet;
        rowList_ = null;
        rowListIndex_ = 0;
        container_ = container;
        tql_ = tql;
        offset_ = 0;
        limit_ = tqlLimit;
        rowCount_ = 0;
    }

    /**
     * <div lang="ja">
     * GridStore#multiGetで読み込んだデータを管理するためのオブジェクトを生成します。
     * @param rowList GridStore#multiGetの処理結果
     * </div><div lang="en">
     * Create an object to manipulate data read by GridStore#multiGet.
     * @param rowList GridStore#multiGet processing results
     * </div>
     */
    public GDRowSet(List<Row> rowList) {
        query_ = null;
        rowSet_ = null;
        rowList_ = rowList;
        rowListIndex_ = 0;
        container_ = null;
        tql_ = null;
        offset_ = 0;
        limit_ = 0;
        rowCount_ = 0;
    }

    /**
     * <div lang="ja">
     * 次のデータを取得できるかを判定します。
     * @return 取得できるロウデータが残っていればtrue、そうでなければfalse
     * @throws GSException GridDBで例外が発生しました
     * </div><div lang="en">
     * Determine whether the next data can be acquired.
     * @return true if acquirable row data remains and false otherwise
     * @throws GSException an exception occurred in GridDB
     * </div>
     */
    public boolean hasNext() throws GSException {
        boolean ret = false;
        if (rowSet_ != null) {
            ret = rowSet_.hasNext();
            if (! ret) {
                if (limit_ > 0 && rowCount_ >= limit_) {
                    rowSet_.close();
                    rowSet_ = null;
                    if (query_ != null) {
                        query_.close();
                        query_ = null;
                    }
                    offset_ += limit_;
                    String tql = tql_ + " offset " + offset_;
                    query_ = container_.query(tql);
                    rowSet_ = query_.fetch(false);
                    ret = rowSet_.hasNext();
                    rowCount_ = 0;
                }
            }
        } else if (rowList_ != null) {
            if (rowListIndex_ < rowList_.size()) {
                ret = true;
            } else {
                ret = false;
            }
        }
        return ret;
    }

    /**
     * <div lang="ja">
     * 次のデータ(GridDBのRowオブジェクト)を返します。
     * @return 取得したロウデータ
     * @throws GSException GridDBで例外が発生しました
     * </div><div lang="en">
     * Return the next data (GridDB Row object).
     * @return acquired row data
     * @throws GSException an exception occurred in GridDB
     * </div>
     */
    public Row nextRow() throws GSException {
        Row ret = null;
        if (rowSet_ != null) {
            ret = (Row) rowSet_.next();
            rowCount_++;
        } else if (rowList_ != null) {
            if (rowListIndex_ < rowList_.size()) {
                ret = rowList_.get(rowListIndex_++);
            }
        }
        return ret;
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
        if (rowSet_ != null) {
            rowSet_.close();
            rowSet_ = null;
        }
        rowList_ = null;
        rowListIndex_ = 0;
    }

}
