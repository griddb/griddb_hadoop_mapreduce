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

package com.toshiba.mwcloud.gs.hadoop.mapreduce.examples.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.ContainerType;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Row;

import com.toshiba.mwcloud.gs.hadoop.mapreduce.examples.GSConf;

public class GSTool {
    private static final String CMD_PREPARE = "prepare";
    private static final String CMD_DELETE = "delete";

    private static final String JOB = "job";
    private static final String INPUT = "input";
    private static final String NUM_CONTAINERS = "num-containers";
    private static final String OUTPUT = "output";

    private static final String[] PREPARE_KEYS = {JOB, INPUT, NUM_CONTAINERS, OUTPUT};
    private static final String[] DELETE_KEYS = {INPUT, NUM_CONTAINERS, OUTPUT};

    private static final String JOB_WORDCOUNT = "wordcount";
    private static final String JOB_APPSEARCH = "appsearch";

    public GSTool() {}

    public void prepare(String[] args) throws IOException {
        List<String> fileNameList = new ArrayList<String>();

        List<String> argList = new ArrayList<String>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].substring(0, 2).equals("--")) {
                argList.add(args[i]);
                argList.add(args[++i]);
            } else {
                fileNameList.add(args[i]);
            }
        }
        args = argList.toArray(new String[0]);

        GSConf gsConf = getGSConf(args, PREPARE_KEYS);
        GridStore gridstore = GridStoreFactory.getInstance().getGridStore(gsConf.getGSProperties());

        dropContainers(gridstore, gsConf);

        List<Container<Object, Row>> containerList = new ArrayList<Container<Object, Row>>();
        List<Row> rowList = new ArrayList<Row>();

        int numContainers = Integer.valueOf(gsConf.get(NUM_CONTAINERS));
        for (int i = 1; i <= numContainers; i++) {
            String name = getInputContainerName(gsConf.get(INPUT), i);
            Container<Object, Row> container = gridstore.putContainer(name, getInputContainerInfo(name), true);
            container.setAutoCommit(true);
            Row row = container.createRow();
            containerList.add(container);
            rowList.add(row);
        }
        gridstore.putContainer(gsConf.get(OUTPUT), getOutputContainerInfo(gsConf.get(OUTPUT), gsConf.get(JOB)), true);

        int count = 1;
        for (String fileName : fileNameList) {
            BufferedReader br = new BufferedReader(new FileReader(new File(fileName)));
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.length() == 0) {
                    continue;
                }
                int n = Math.abs(line.hashCode()) % containerList.size();
                Container<Object, Row> container = containerList.get(n);
                Row row = rowList.get(n);
                row.setInteger(0, count++);
                row.setString(1, line);
                container.put(row);
            }
            br.close();
        }
        for (Container<Object, Row> container : containerList) {
            container.close();
        }

        gridstore.close();
    }

    public void delete(String[] args) throws IOException {
        GSConf gsConf = getGSConf(args, DELETE_KEYS);
        GridStore gridstore = GridStoreFactory.getInstance().getGridStore(gsConf.getGSProperties());

        dropContainers(gridstore, gsConf);

        gridstore.close();
    }

    private void dropContainers(GridStore gridstore, GSConf gsConf) throws IOException {
        int numContainers = Integer.valueOf(gsConf.get(NUM_CONTAINERS));
        for (int i = 1; i <= numContainers; i++) {
            String name = getInputContainerName(gsConf.get(INPUT), i);
            if (gridstore.getContainer(name) != null) {
                gridstore.dropContainer(name);
            }
        }
        if (gridstore.getContainer(gsConf.get(OUTPUT)) != null) {
            gridstore.dropContainer(gsConf.get(OUTPUT));
        }
    }

    private GSConf getGSConf(String[] args, String[] keys) throws IOException {
        GSConf gsConf = new GSConf();
        for (String key : keys) {
            gsConf.addEntry(key);
        }
        gsConf.parseArg(args);
        for (String key : keys) {
            if (gsConf.get(key) == null) {
                throw new IOException("Not defined(" + key + ")");
            }
        }
        return gsConf;
    }

    private String getInputContainerName(String name, int n) {
        return name + "_" + n;
    }

    private ContainerInfo getInputContainerInfo(String name) throws IOException {
        List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
        columnList.add(new ColumnInfo("id", GSType.INTEGER));
        columnList.add(new ColumnInfo("text", GSType.STRING));
        return new ContainerInfo(name, ContainerType.COLLECTION, columnList, true);
    }

    private ContainerInfo getOutputContainerInfo(String name, String job) throws IOException {
        List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
        if (job.equalsIgnoreCase(JOB_WORDCOUNT)) {
            columnList.add(new ColumnInfo("word", GSType.STRING));
            columnList.add(new ColumnInfo("count", GSType.INTEGER));
        } else if (job.equalsIgnoreCase(JOB_APPSEARCH)) {
            columnList.add(new ColumnInfo("id", GSType.INTEGER));
            columnList.add(new ColumnInfo("text", GSType.STRING));
            columnList.add(new ColumnInfo("distance", GSType.INTEGER_ARRAY));
            columnList.add(new ColumnInfo("begin", GSType.INTEGER_ARRAY));
            columnList.add(new ColumnInfo("end", GSType.INTEGER_ARRAY));
        } else {
            throw new IOException("Unknown job(" + job + ")");
        }
        return new ContainerInfo(name, ContainerType.COLLECTION, columnList, true);
    }

    public static void main(String[] args) throws IOException {
        String cmd = args[0];

        GSTool tool = new GSTool();
        if (CMD_PREPARE.equalsIgnoreCase(cmd)) {
            tool.prepare(Arrays.copyOfRange(args, 1, args.length));
        } else if (CMD_DELETE.equalsIgnoreCase(cmd)) {
            tool.delete(Arrays.copyOfRange(args, 1, args.length));
        } else {
            throw new IOException("Unknown command(" + cmd + ")");
        }
    }
}
