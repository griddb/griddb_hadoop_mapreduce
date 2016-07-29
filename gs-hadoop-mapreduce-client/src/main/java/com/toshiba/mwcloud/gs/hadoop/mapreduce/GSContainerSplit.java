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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * <div lang="ja">
 * GridDB用のInputSplitです。
 * </div><div lang="en">
 * InputSplit of GridDB connector.
 * </div>
 */
public class GSContainerSplit extends InputSplit implements Writable, Comparable<GSContainerSplit> {
    private final com.toshiba.mwcloud.gs.hadoop.mapred.GSContainerSplit delegate_;

    public GSContainerSplit() {
        delegate_ = new com.toshiba.mwcloud.gs.hadoop.mapred.GSContainerSplit();
    }

    /**
     * <div lang="ja">
     * 入力対象のパーティションとコンテナの情報からInputSplitを生成します。
     * @param partitionHostList 入力対象のパーティション毎に選択されたホスト名配列
     * @param containerNameList パーティション毎に作成された入力対象のコンテナ名配列
     * </div><div lang="en">
     * Generate InputSplits from containers and partitions for input processing.
     * @param partitionHostList host name array selected for each partition
     * @param containerNameList container name array of each partition
     * </div>
     */
    public GSContainerSplit(String[] partitionHostList, String[][] containerNameList) {
        delegate_ = new com.toshiba.mwcloud.gs.hadoop.mapred.GSContainerSplit(partitionHostList, containerNameList);
    }

    /**
     * <div lang="ja">
     * {@inheritDoc}
     * @see Writable#readFields(DataInput)
     * @throws IOException DataInputオブジェクトからの読み込み処理でエラーが発生しました
     * </div><div lang="en">
     * {@inheritDoc}
     * @see Writable#readFields(DataInput)
     * @throws IOException error occurred when reading from DataInput object
     * </div>
     */
    @Override
    public void readFields(final DataInput in) throws IOException {
        delegate_.readFields(in);
    }

    /**
     * <div lang="ja">
     * {@inheritDoc}
     * @see Writable#write(DataOutput)
     * @throws IOException DataOutputオブジェクトへの書き込み処理でエラーが発生しました
     * </div><div lang="en">
     * {@inheritDoc}
     * @see Writable#write(DataOutput)
     * @throws IOException error occurred when writing to DataOutput object
     * </div>
     */
    @Override
    public void write(final DataOutput out) throws IOException {
        delegate_.write(out);
    }

    /*
     * (non Javadoc)
     * @see org.apache.hadoop.mapreduce.InputSplit#getLength()
     */
    @Override
    public long getLength() {
        return delegate_.getLength();
    }

    /*
     * (non Javadoc)
     * @see org.apache.hadoop.mapreduce.InputSplit#getLocations()
     */
    @Override
    public String[] getLocations() {
        return delegate_.getLocations();
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return delegate_.toString();
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(GSContainerSplit o) {
        return delegate_.compareTo(o.delegate_);
    }

    /*
     * (non Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return delegate_.hashCode();
    }

    /*
     * (non Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof GSContainerSplit)) {
            return false;
        }
        GSContainerSplit other = (GSContainerSplit) obj;
        return delegate_.equals(other.delegate_);
    }

    /**
     * <div lang="ja">
     * 委譲先オブジェクトを返します。
     * @return com.toshiba.mwcloud.mapred.GSContainerSplitオブジェクト
     * </div><div lang="en">
     * Return the transfer destination object.
     * @return com.toshiba.mwcloud.mapred.GSContainerSplit object
     * </div>
     */
    public com.toshiba.mwcloud.gs.hadoop.mapred.GSContainerSplit getDelegate() {
        return delegate_;
    }
}
