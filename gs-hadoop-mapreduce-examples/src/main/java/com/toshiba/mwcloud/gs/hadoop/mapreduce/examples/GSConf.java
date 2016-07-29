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

package com.toshiba.mwcloud.gs.hadoop.mapreduce.examples;

import static com.toshiba.mwcloud.gs.hadoop.conf.GDProperty.*;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import com.toshiba.mwcloud.gs.hadoop.conf.GDProperty;

public class GSConf {
    public static final String PROP_CHARSET = "gs.charset";
    private static final String DEFAULT_CHARSET = "UTF-8";

    private class Entry {
        private String key_;
        private String value_;

        public Entry(String key) {
            key_ = key;
        }
        public Entry(String key, String value) {
            key_ = key;
            value_ = value;
        }

        public void setValue(String value) {
            value_ = value;
        }
        public String getKey() {
            return key_;
        }
        public String getValue() {
            return value_;
        }
    }

    private class GSEntry extends Entry {
        private GDProperty prop_;

        public GSEntry(String key, GDProperty prop, String value) {
            super(key, value);
            prop_ = prop;
        }

        public GDProperty getProp() {
            return prop_;
        }
    }

    private class AuxEntry extends Entry {
        private String prop_;

        public AuxEntry(String key, String prop) {
            super(key);
            prop_ = prop;
        }

        public String getProp() {
            return prop_;
        }
    }

    private Map<String, Entry> entryMap_;
    private List<String> containerNameList_;

    private GSEntry notificationAddress_;
    private GSEntry notificationPort_;
    private GSEntry clusterName_;
    private GSEntry host_;
    private GSEntry port_;
    private GSEntry user_;
    private GSEntry password_;

    private List<GSEntry> gsOptions_;

    private String charset_ = DEFAULT_CHARSET;

    public GSConf() {
        entryMap_ = new HashMap<String, Entry>();
        containerNameList_ = new ArrayList<String>();

        notificationAddress_ = createGSEntry("notificationAddress", GS_NOTIFICATION_ADDRESS, "239.0.0.1");
        notificationPort_ = createGSEntry("notificationPort", GS_NOTIFICATION_PORT, "31999");
        clusterName_ = createGSEntry("clusterName", GS_CLUSTER_NAME);
        host_ = createGSEntry("host", GS_HOST);
        port_ = createGSEntry("port", GS_PORT, "10001");
        user_ = createGSEntry("user", GS_USER);
        password_ = createGSEntry("password", GS_PASSWORD);

        gsOptions_ = new ArrayList<GSEntry>();

        gsOptions_.add(createGSEntry("consistency", GS_CONSISTENCY));
        gsOptions_.add(createGSEntry("transactionTimeout", GS_TRANSACTION_TIMEOUT));
        gsOptions_.add(createGSEntry("failoverTimeout", GS_FAILOVER_TIMEOUT));
        gsOptions_.add(createGSEntry("containerCacheSize", GS_CONTAINER_CACHE_SIZE));
    }
    private GSEntry createGSEntry(String key, GDProperty prop) {
        return createGSEntry(key, prop, null);
    }
    private GSEntry createGSEntry(String key, GDProperty prop, String value) {
        GSEntry entry = new GSEntry(key, prop, value);
        entryMap_.put(key, entry);
        return entry;
    }

    public void setCharset(String charset) {
        charset_ = charset;
    }
    public String getCharset() {
        return charset_;
    }

    public void addEntry(String key) {
        Entry entry = new Entry(key);
        entryMap_.put(key, entry);
    }
    public void addAuxEntry(String key, String prop) {
        AuxEntry entry = new AuxEntry(key, prop);
        entryMap_.put(key, entry);
    }

    public void parseArg(String[] args) throws IOException {
        for (int i = 0; i < args.length; i++) {
            if ("--file".equals(args[i])) {
                load(args[++i]);
            } else if ("--define".equals(args[i])) {
                set(args[++i]);
            } else if (args[i].substring(0, 2).equals("--")) {
                set(args[i].substring(2), args[i + 1]);
                i++;
            } else {
                containerNameList_.add(args[i]);
            }
        }
    }
    private void load(String fileName) throws IOException {
        Properties props = new Properties();
        FileReader fr = new FileReader(new File(fileName));
        props.load(fr);
        fr.close();
        for (Entry entry : entryMap_.values()) {
            String value = props.getProperty(entry.getKey());
            if (value != null) {
                entry.setValue(value);
            }
        }
    }
    private void set(String arg) throws IOException {
        String[] pair = arg.split("=");
        if (pair.length != 2) {
            throw new IOException("Illegal define format(" + arg + ")");
        }
        String key = pair[0].trim();
        String value = pair[1].trim();
        if (key.length() == 0 || value.length() == 0) {
            throw new IOException("Illegal define format(" + arg + ")");
        }
        set(key, value);
    }
    private void set(String key, String value) throws IOException {
        Entry entry = entryMap_.get(key);
        if (entry == null) {
            throw new IOException("Unknown key(" + key + ")");
        }
        entry.setValue(value);
    }

    public String get(String key) {
        Entry entry = entryMap_.get(key);
        if (entry == null) {
            return null;
        }
        return entry.getValue();
    }

    public void setup(Configuration conf) throws IOException {
        setupForGSEntries(conf);
        setupForAuxEntries(conf);
        setupForContainerNames(conf);
    }
    private void setupForGSEntries(Configuration conf) throws IOException {
        if (user_.getValue() == null) {
            throw new IOException("Not defined(" + user_.getKey() + ")");
        } else if (password_.getValue() == null) {
            throw new IOException("Not defined(" + password_.getKey() + ")");
        }
        if (host_.getValue() == null) {
            if (clusterName_.getValue() == null) {
                throw new IOException("Not defined(" + clusterName_.getKey() + ")");
            }
            notificationAddress_.getProp().set(notificationAddress_.getValue(), conf);
            notificationPort_.getProp().set(notificationPort_.getValue(), conf);
            clusterName_.getProp().set(clusterName_.getValue(), conf);
        } else {
            host_.getProp().set(host_.getValue(), conf);
            port_.getProp().set(port_.getValue(), conf);
            if (clusterName_.getValue() != null) {
                clusterName_.getProp().set(clusterName_.getValue(), conf);
            }
        }
        user_.getProp().set(user_.getValue(), conf);
        password_.getProp().set(password_.getValue(), conf);
        for (GSEntry entry : gsOptions_) {
            if (entry.getValue() != null) {
                entry.getProp().set(entry.getValue(), conf);
            }
        }
    }
    private void setupForAuxEntries(Configuration conf) throws IOException {
        for (Entry entry : entryMap_.values()) {
            if (entry instanceof AuxEntry) {
                AuxEntry auxEntry = (AuxEntry) entry;
                if (auxEntry.getValue() == null) {
                    throw new IOException("Not defined(" + auxEntry.getKey() + ")");
                }
                conf.set(auxEntry.getProp(), URLEncoder.encode(auxEntry.getValue(), charset_));
                if (conf.get(PROP_CHARSET) == null) {
                    conf.set(PROP_CHARSET, charset_);
                }
            }
        }
    }
    private void setupForContainerNames(Configuration conf) throws IOException {
        if (containerNameList_.size() < 2) {
            throw new IOException("Not specified(input container(s) and/or output container)");
        }
        if (containerNameList_.size() > 2) {
            StringBuffer sb = new StringBuffer(containerNameList_.get(0));
            for (int i = 1; i < containerNameList_.size() - 1; i++) {
                sb.append("," + containerNameList_.get(i));
            }
            GS_INPUT_CONTAINER_NAME_LIST.set(sb.toString(), conf);
        } else {
            GS_INPUT_CONTAINER_NAME_REGEX.set(containerNameList_.get(0), conf);
        }
        GS_OUTPUT_CONTAINER_NAME.set(getOutputContainerName(), conf);
    }

    public String getOutputContainerName() {
        return containerNameList_.get(containerNameList_.size() - 1);
    }

    public Properties getGSProperties() throws IOException {
        Properties props = new Properties();
        if (user_.getValue() == null) {
            throw new IOException("Not defined(" + user_.getKey() + ")");
        } else if (password_.getValue() == null) {
            throw new IOException("Not defined(" + password_.getKey() + ")");
        }
        if (host_.getValue() == null) {
            if (clusterName_.getValue() == null) {
                throw new IOException("Not defined(" + clusterName_.getKey() + ")");
            }
            props.setProperty(notificationAddress_.getKey(), notificationAddress_.getValue());
            props.setProperty(notificationPort_.getKey(), notificationPort_.getValue());
            props.setProperty(clusterName_.getKey(), clusterName_.getValue());
        } else {
            props.setProperty(host_.getKey(), host_.getValue());
            props.setProperty(port_.getKey(), port_.getValue());
            if (clusterName_.getValue() != null) {
                props.setProperty(clusterName_.getKey(), clusterName_.getValue());
            }
        }
        props.setProperty(user_.getKey(), user_.getValue());
        props.setProperty(password_.getKey(), password_.getValue());
        for (GSEntry entry : gsOptions_) {
            if (entry.getValue() != null) {
                props.setProperty(entry.getKey(), entry.getValue());
            }
        }
        return props;
    }
}
