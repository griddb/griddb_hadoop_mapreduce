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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;

import static com.toshiba.mwcloud.gs.hadoop.conf.GDProperty.*;

import com.toshiba.mwcloud.gs.hadoop.conf.GDPropertyUtils;
import com.toshiba.mwcloud.gs.hadoop.mapreduce.GSContainerSplit;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.PartitionController;

/**
 * <div lang="ja">
 * GridDB用InputFormatクラス共通の処理を実行します。
 * </div><div lang="en">
 * Common processing for GridDB InputFormats.
 * </div>
 */
public class GDInputFormat {
    /**
     * <div lang="ja">
     * org.apache.hadoop.mapred.InputSplit用のgetSplits処理です。<br/>
     * 返却するInputSplitオブジェクトの個数は入力対象のパーティション数と引数numSplitsで指定された値の小さい方になります。<br/>
     * 各InputSplitオブジェクトには、対応するMapタスクで処理するパーティションとコンテナ情報が格納されます。<br/>
     * 引数numSplitsの値が入力対象のパーティション数より小さい場合は、複数パーティションを処理対象とするInputSplitオブジェクトが生成されます。
     * @param numSplits スプリット数
     * @param conf Configurationオブジェクト
     * @return InputSplitオブジェクトの配列
     * @throws GSException GridDBで例外が発生しました
     * </div><div lang="en">
     * getSplits method for org.apache.hadoop.mapred.InputSplit.<br/>
     * The number of InputSplit objects to return will be the smaller of the number of partitions for input processing and the value specified in the argument numSplits.<br/>
     * The partition and container information to be processed in the corresponding Map task is stored in each InputSplit object.<br/>
     * If the value of the argument numSplits is smaller than the number of partitions, an InputSplit object targeting multiple partitions for processing is generated.
     * @param numSplits number of splits
     * @param conf Configuration object
     * @return InputSplit object array
     * @throws GSException an exception occurred in GridDB
     * </div>
     */
    public org.apache.hadoop.mapred.InputSplit[] getSplitArray(int numSplits, Configuration conf) throws GSException {
        Properties prop = GDPropertyUtils.getGSProp(conf, GDPropertyUtils.getGSPropertiesToRead());
        GridStore gridstore = GridStoreFactory.getInstance().getGridStore(prop);
        PartitionController controller = gridstore.getPartitionController();

        GSContainerSplit[] gsSplits = getSplits(conf, controller, numSplits);
        gridstore.close();
        if (gsSplits == null) {
            return null;
        }
        org.apache.hadoop.mapred.InputSplit[] splits = new org.apache.hadoop.mapred.InputSplit[gsSplits.length];
        for (int i = 0; i < gsSplits.length; i++) {
            splits[i] = gsSplits[i].getDelegate();
        }
        return splits;
    }

    /**
     * <div lang="ja">
     * org.apache.hadoop.mapreduce.InputSplit用のgetSplits処理です。<br/>
     * 返却するInputSplitオブジェクトの個数は入力対象のパーティション数と引数numSplitsで指定された値の小さい方になります。<br/>
     * 各InputSplitオブジェクトには、対応するMapタスクで処理するパーティションとコンテナ情報が格納されます。<br/>
     * 引数numSplitsの値が入力対象のパーティション数より小さい場合は、複数パーティションを処理対象とするInputSplitオブジェクトが生成されます。
     * @param numSplits スプリット数
     * @param conf Configurationオブジェクト
     * @return InputSplitオブジェクトのリスト
     * @throws GSException GridDBで例外が発生しました
     * </div><div lang="en">
     * getSplits method for org.apache.hadoop.mapreduce.InputSplit.<br/>
     * The number of InputSplit objects to return will be the smaller of the number of partitions for input processing and the value specified in the argument numSplits.<br/>
     * The partition and container information to be processed in the corresponding Map task is stored in each InputSplit object.<br/>
     * If the value of the argument numSplits is smaller than the number of partitions subject to input, an InputSplit object targeting multiple partitions for processing is generated.
     * @param numSplits number of splits
     * @param conf Configuration object
     * @return InputSplit object list
     * @throws GSException an exception occurred in GridDB
     * </div>
     */
    public List<org.apache.hadoop.mapreduce.InputSplit> getSplitList(int numSplits, Configuration conf) throws GSException {
        Properties prop = GDPropertyUtils.getGSProp(conf, GDPropertyUtils.getGSPropertiesToRead());
        GridStore gridstore = GridStoreFactory.getInstance().getGridStore(prop);
        PartitionController controller = gridstore.getPartitionController();

        GSContainerSplit[] gsSplits = getSplits(conf, controller, numSplits);
        gridstore.close();
        if (gsSplits == null) {
            return null;
        }
        List<org.apache.hadoop.mapreduce.InputSplit> splits = new ArrayList<org.apache.hadoop.mapreduce.InputSplit>();
        for (int i = 0; i < gsSplits.length; i++) {
            splits.add(gsSplits[i]);
        }
        return splits;
    }

    private GSContainerSplit[] getSplits(Configuration conf, PartitionController controller, int numSplits) throws GSException {
        List<String> containerNames = getContainerNameList(conf, controller);
        if (containerNames == null) {
            return null;
        }

        List<List<String>> partitionInfoList = new ArrayList<List<String>>();
        for (int i = 0; i < controller.getPartitionCount(); i++) {
            partitionInfoList.add(new ArrayList<String>());
        }
        for (String containerName : containerNames) {
            int j = controller.getPartitionIndexOfContainer(containerName);
            partitionInfoList.get(j).add(containerName);
        }

        int partitionListLength = 0;
        for (List<String> list : partitionInfoList) {
            if (list.size() > 0) {
                partitionListLength++;
            }
        }

        int[] partitionNoList = new int[partitionListLength];
        String[][] containerNameList = new String[partitionListLength][];

        int partitionListIndex = 0;
        int partitionNo = 0;
        for (List<String> list : partitionInfoList) {
            if (list.size() > 0) {
                partitionNoList[partitionListIndex] = partitionNo;
                containerNameList[partitionListIndex] = list.toArray(new String[list.size()]);
                partitionListIndex++;
            }
            partitionNo++;
        }

        GSContainerSplit[] splits = createSplits(partitionNoList, containerNameList, conf, controller, numSplits);

        return splits;
    }

    private List<String> getContainerNameList(Configuration conf, PartitionController controller) throws GSException {
        List<String> containerNameList;

        String regex = GS_INPUT_CONTAINER_NAME_REGEX.get(conf);
        if (regex == null) {
            containerNameList = GDPropertyUtils.getPropertyList(conf, GS_INPUT_CONTAINER_NAME_LIST);
            if (containerNameList.size() > 0) {
                return containerNameList;
            }

            throw new GSException("'" + GS_INPUT_CONTAINER_NAME_LIST.getKey() + "' or '" + GS_INPUT_CONTAINER_NAME_REGEX.getKey() + "' must be defined.");
        }

        Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);

        containerNameList = new ArrayList<String>();
        int partitionCount = controller.getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            List<String> containerNameListInPartition = controller.getContainerNames(i, 0, null);
            for (String containerName : containerNameListInPartition) {
                Matcher m = p.matcher(containerName);
                if (! m.matches()) {
                    continue;
                }
                if (m.start() == 0 && m.end() == containerName.length()) {
                    containerNameList.add(containerName);
                }
            }
        }

        if (containerNameList.size() == 0) {
            throw new GSException("'" + regex + "' doesn't match any container names");
        }

        return containerNameList;
    }

    private GSContainerSplit[] createSplits(int[] partitionNoList, String[][] containerNameList,
            Configuration conf, PartitionController controller, int numSplits) throws GSException {
        String[] partitionHostList = new String[partitionNoList.length];

        HashMap<String, Integer> hostMap = new HashMap<String, Integer>();
        for (int i = 0; i < partitionNoList.length; i++) {
            partitionHostList[i] = getHost(controller, partitionNoList[i], hostMap);
        }

        GSContainerSplit[] splits = null;

        if (partitionNoList.length > numSplits) {
            if (numSplits <= 0) {
                numSplits = 1;
            }

            int step = partitionNoList.length / numSplits;
            int remainder = partitionNoList.length % numSplits;

            splits = new GSContainerSplit[numSplits];
            int index = 0;

            for (int i = 0; i < numSplits; i++) {
                int length = step;
                if (i < remainder) {
                    length += 1;
                }

                String[] splitPartitionHostList = new String[length];
                String[][] splitContainerNameList = new String[length][];

                for (int j = 0; j < length; j++) {
                    splitPartitionHostList[j] = partitionHostList[index];
                    splitContainerNameList[j] = containerNameList[index];
                    index++;
                }

                splits[i] = new GSContainerSplit(splitPartitionHostList, splitContainerNameList);
            }
        } else {
            splits = new GSContainerSplit[partitionNoList.length];

            for (int i = 0; i < partitionNoList.length; i++) {
                String[] splitPartitionHostList = new String[1];
                String[][] splitContainerNameList = new String[1][];

                splitPartitionHostList[0] = partitionHostList[i];
                splitContainerNameList[0] = containerNameList[i];

                splits[i] = new GSContainerSplit(splitPartitionHostList, splitContainerNameList);
            }
        }

        return splits;
    }

    private String getHost(PartitionController controller, int index, HashMap<String, Integer> hostMap) throws GSException {
        List<InetAddress> list = controller.getHosts(index);

        int[] freq = new int[list.size()];
        for (int i = 0; i < list.size(); i++) {
            Integer ii = hostMap.get(list.get(i).getCanonicalHostName());
            if (ii == null) {
                freq[i] = 0;
            } else {
                freq[i] = ii;
            }
        }

        int minFreq = Integer.MAX_VALUE;
        int minPos = -1;
        for (int i = 0; i < list.size(); i++) {
            if (minFreq > freq[i]) {
                minFreq = freq[i];
                minPos = i;
            }
        }
        if (minPos < 0) {
            minPos = 0;
        }

        String addr = list.get(minPos).getCanonicalHostName();
        hostMap.put(addr, freq[minPos] + 1);

        return addr;
    }
}
