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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;

/**
 * <div lang="ja">
 * GridDB用のInputSplitです。
 * </div><div lang="en">
 * InputSplit of GridDB connector.
 * </div>
 */
public class GSContainerSplit implements InputSplit, Comparable<GSContainerSplit> {
    private int containerInfoLength_;

    private String[] partitionHost_;
    private int[] containerNameListLength_;
    private String[][] containerNameList_;

    public GSContainerSplit() {
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
        containerInfoLength_ = containerNameList.length;
        partitionHost_ = partitionHostList;
        containerNameList_ = containerNameList;

        containerNameListLength_ = null;
        if (containerNameList_ != null) {
            containerNameListLength_ = new int[containerNameList_.length];
            for (int i = 0; i < containerNameList_.length; i++) {
                if (containerNameList_[i] != null) {
                    containerNameListLength_[i] = containerNameList_[i].length;
                }
            }
        }
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
        containerInfoLength_ = in.readInt();

        if (containerInfoLength_ > 0) {
            partitionHost_ = new String[containerInfoLength_];
            containerNameListLength_ = new int[containerInfoLength_];
            containerNameList_ = new String[containerInfoLength_][];

            for (int i = 0; i < containerInfoLength_; i++) {
                partitionHost_[i] = Text.readString(in);
                containerNameListLength_[i] = in.readInt();

                if (containerNameListLength_[i] > 0) {
                    containerNameList_[i] = new String[containerNameListLength_[i]];

                    for (int j = 0; j < containerNameListLength_[i]; j++) {
                        containerNameList_[i][j] = Text.readString(in);
                    }
                }
            }
        }
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
        out.writeInt(containerInfoLength_);

        for (int i = 0; i < containerInfoLength_; i++) {
            Text.writeString(out, partitionHost_[i]);
            out.writeInt(containerNameListLength_[i]);

            for (int j = 0; j < containerNameListLength_[i]; j++) {
                Text.writeString(out, containerNameList_[i][j]);
            }
        }
    }

    /*
     * (non Javadoc)
     * @see org.apache.hadoop.mapred.InputSplit#getLength()
     */
    @Override
    public long getLength() {
        return containerInfoLength_;
    }

    /**
     * <div lang="ja">
     * 入力対象の各パーティションに対応するホスト名配列を返します。
     * @see InputSplit#getLocations()
     * </div><div lang="en">
     * Return the host name array corresponding to each partition for input processing.
     * @see InputSplit#getLocations()
     * </div>
     */
    @Override
    public String[] getLocations() {
        return partitionHost_;
    }

    /**
     * <div lang="ja">
     * パーティション毎のコンテナ名配列を返します。
     * @return パーティション毎のコンテナ名配列
     * </div><div lang="en">
     * Return the container name array of each partition.
     * @return container name array of each partition
     * </div>
     */
    public String[][] getContainerNameList() {
        return containerNameList_;
    }

    private String getHeadPartitionHost() {
        if (containerInfoLength_ > 0) {
            return partitionHost_[0];
        }
        return "";
    }

    private String getHeadContainerName() {
        if (containerInfoLength_ > 0) {
            if (containerNameListLength_[0] > 0) {
                return containerNameList_[0][0];
            }
        }
        return "";
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getHeadPartitionHost());

        String containerName = getHeadContainerName();
        if (containerName.length() > 0) {
            sb.append(":");
            sb.append(containerName);
        }

        return sb.toString();
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(GSContainerSplit o) {
        int ret = 0;

        if (ret == 0) {
            ret = getHeadPartitionHost().compareTo(o.getHeadPartitionHost());
        }
        if (ret == 0) {
            ret = getHeadContainerName().compareTo(o.getHeadContainerName());
        }

        return ret;
    }

    /*
     * (non Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;

        result = prime * result + getHeadPartitionHost().hashCode();
        result = prime * result + getHeadContainerName().hashCode();

        return result;
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

        if (! getHeadPartitionHost().equals(other.getHeadPartitionHost())) {
            return false;
        }
        if (! getHeadContainerName().equals(other.getHeadContainerName())) {
            return false;
        }

        return true;
    }
}
