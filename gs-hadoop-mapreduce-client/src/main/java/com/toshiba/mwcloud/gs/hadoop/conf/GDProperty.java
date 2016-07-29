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

import org.apache.hadoop.conf.Configuration;

/**
 * <div lang="ja">
 * GridDBコネクタで用いるプロパティに対応した列挙型です。<br/>
 * 各定数を通して対応するプロパティの値の設定や取得を行うことができます。<br/>
 * 値の設定や取得の際には書式の検証や正規化の処理を行います。
 * </div><div lang="en">
 * Enumerated constants corresponding to the properties used in the GridDB connector.<br/>
 * The value of the corresponding property can be set or acquired through each object.<br/>
 * When setting or acquiring the value, verification and normalization of the value are performed.
 * </div>
 */
public enum GDProperty {
    /**
     * <div lang="ja">
     * プロパティgs.hostに対応した定数です。<br/>
     * 接続先GridDBマスタのホスト名、あるいは、IPアドレスを指定するための入出力処理共用定 数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.host".<br/>
     * This object is used to specify the host name or IP address of the GridDB master for input and/or output processing.
     * </div>
     */
    GS_HOST("gs.host", "", null, null) {
        @Override
        public boolean validate(String value) {
            return true;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.inout.hostに対応した定数です。<br/>
     * 接続先GridDBマスタのホスト名、あるいは、IPアドレスを指定するための入力処理用定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.inout.host".<br/>
     * This object is used to specify the host name or IP address of the GridDB master for input processing.
     * </div>
     */
    GS_INPUT_HOST("gs.input.host", "", "host", GS_HOST) {
        @Override
        public boolean validate(String value) {
            return true;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.output.hostに対応した定数です。<br/>
     * 接続先GridDBマスタのホスト名、あるいは、IPアドレスを指定するための出力処理用定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.output.host".<br/>
     * This object is used to specify the host name or IP address of the GridDB master for output processing.
     * </div>
     */
    GS_OUTPUT_HOST("gs.output.host", "", "host", GS_HOST) {
        @Override
        public boolean validate(String value) {
            return true;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },

    /**
     * <div lang="ja">
     * プロパティgs.portに対応した定数です。<br/>
     * 接続先GridDBマスタのポート番号を指定するための入出力処理共用定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.port".<br/>
     * This object is used to specify the port number of the GridDB master for input and/or output processing.
     * </div>
     */
    GS_PORT("gs.port", "", null, null) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            try {
                return Short.valueOf(value) >= 0;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.input.portに対応した定数です。<br/>
     * 接続先GridDBマスタのポート番号を指定するための入力処理用定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.input.port".<br/>
     * This object is used to specify the port number of the GridDB master for input processing.
     * </div>
     */
    GS_INPUT_PORT("gs.input.port", "", "port", GS_PORT) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            try {
                return Short.valueOf(value) >= 0;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.output.portに対応した定数です。<br/>
     * 接続先GridDBマスタのポート番号を指定するための出力処理用定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.output.port".<br/>
     * This object is used to specify the port number of the GridDB master for output processing.
     * </div>
     */
    GS_OUTPUT_PORT("gs.output.port", "", "port", GS_PORT) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            try {
                return Short.valueOf(value) >= 0;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },

    /**
     * <div lang="ja">
     * プロパティgs.notification.addressに対応した定数です。<br/>
     * GridDBマスタの自動検出に用いられる通知情報を受信するためのIPアドレスを指定するための入出力処理共用定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.notification.address".<br/>
     * This object is used to specify the IP address used in the automatic detection of the GridDB master for input and/or output processing.
     * </div>
     */
    GS_NOTIFICATION_ADDRESS("gs.notification.address", "", null, null) {
        @Override
        public boolean validate(String value) {
            return true;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.input.notification.addressに対応した定数です。<br/>
     * GridDBマスタの自動検出に用いられる通知情報を受信するためのIPアドレスを指定するための入力処理用定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.input.notification.address".<br/>
     * This object is used to specify the IP address used in the automatic detection of the GridDB master for input processing.
     * </div>
     */
    GS_INPUT_NOTIFICATION_ADDRESS("gs.input.notification.address", "", "notificationAddress", GS_NOTIFICATION_ADDRESS) {
        @Override
        public boolean validate(String value) {
            return true;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.output.notification.addressに対応した定数です。<br/>
     * GridDBマスタの自動検出に用いられる通知情報を受信するためのIPアドレスを指定するための出力処理用定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.output.notification.address".<br/>
     * This object is used to specify the IP address used in the automatic detection of the GridDB master for output processing.
     * </div>
     */
    GS_OUTPUT_NOTIFICATION_ADDRESS("gs.output.notification.address", "", "notificationAddress", GS_NOTIFICATION_ADDRESS) {
        @Override
        public boolean validate(String value) {
            return true;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },

    /**
     * <div lang="ja">
     * プロパティgs.notification.portに対応した定数です。<br/>
     * GridDBマスタの自動検出に用いられる通知情報を 受信するためのポート番号を指定するための入出力処理共用定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.notification.port".<br/>
     * This object is used to specify the port number used in the automatic detection of the GridDB master for input and/or output processing.
     * </div>
     */
    GS_NOTIFICATION_PORT("gs.notification.port", "", null, null) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            try {
                return Short.valueOf(value) >= 0;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.input.notification.portに対応した定数です。<br/>
     * GridDBマスタの自動検出に用いられる通知情報を 受信するためのポート番号を指定するための入力処理用定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.input.notification.port".<br/>
     * This object is used to specify the port number used in the automatic detection of the GridDB master for input processing.
     * </div>
     */
    GS_INPUT_NOTIFICATION_PORT("gs.input.notification.port", "", "notificationPort", GS_NOTIFICATION_PORT) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            try {
                return Short.valueOf(value) >= 0;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.output.notification.portに対応した定数です。<br/>
     * GridDBマスタの自動検出に用いられる通知情報を 受信するためのポート番号を指定するための出力処理用定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.output.notification.port".<br/>
     * This object is used to specify the port number used in the automatic detection of the GridDB master for output processing.
     * </div>
     */
    GS_OUTPUT_NOTIFICATION_PORT("gs.output.notification.port", "", "notificationPort", GS_NOTIFICATION_PORT) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            try {
                return Short.valueOf(value) >= 0;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },

    /**
     * <div lang="ja">
     * プロパティgs.cluster.nameに対応した定数です。<br/>
     * GridDBのクラスタ名を指定するための入出力処理共用定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.cluster.name".<br/>
     * This object is used to specify the GridDB cluster name for input and/or output processing.
     * </div>
     */
    GS_CLUSTER_NAME("gs.cluster.name", "", null, null) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            return value.length() > 0;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.input.cluster.nameに対応した定数です。<br/>
     * GridDBのクラスタ名を指定するための入力処理用定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.input.cluster.name".<br/>
     * This object is used to specify the GridDB cluster name for input processing.
     * </div>
     */
    GS_INPUT_CLUSTER_NAME("gs.input.cluster.name", "", "clusterName", GS_CLUSTER_NAME) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            return value.length() > 0;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.output.cluster.nameに対応した定数です。<br/>
     * GridDBのクラスタ名を指定するための出力処理用定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.output.cluster.name".<br/>
     * This object is used to specify the GridDB cluster name for output processing.
     * </div>
     */
    GS_OUTPUT_CLUSTER_NAME("gs.output.cluster.name", "", "clusterName", GS_CLUSTER_NAME) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            return value.length() > 0;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },

    /**
     * <div lang="ja">
     * プロパティgs.userに対応した定数です。<br/>
     * GridDBのユーザ名を指定するための入出力処理共用定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.user".<br/>
     * This object is used to specify the GridDB user name for input and/or output processing.
     * </div>
     */
    GS_USER("gs.user", null, null, null) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            return value.length() > 0;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.input.userに対応した定数です。<br/>
     * GridDBのユーザ名を指定するための入力処理用定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.input.user".<br/>
     * This object is used to specify the GridDB user name for input processing.
     * </div>
     */
    GS_INPUT_USER("gs.input.user", null, "user", GS_USER) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            return value.length() > 0;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.output.userに対応した定数です。<br/>
     * GridDBのユーザ名を指定するための出力処理用定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.output.user".<br/>
     * This object is used to specify the GridDB user name for output processing.
     * </div>
     */
    GS_OUTPUT_USER("gs.output.user", null, "user", GS_USER) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            return value.length() > 0;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },

    /**
     * <div lang="ja">
     * プロパティgs.passwordに対応した定数です。<br/>
     * GridDBのパスワードを指定するための入出力処理共用定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.password".<br/>
     * This object is used to specify the GridDB password for input and/or output processing.
     * </div>
     */
    GS_PASSWORD("gs.password", null, null, null) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            return value.length() > 0;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.input.passwordに対応した定数です。<br/>
     * GridDBのパスワードを指定するための入力処理用定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.input.password".<br/>
     * This object is used to specify the GridDB password for input processing.
     * </div>
     */
    GS_INPUT_PASSWORD("gs.input.password", null, "password", GS_PASSWORD) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            return value.length() > 0;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.output.passwordに対応した定数です。<br/>
     * GridDBのパスワードを指定するための出力処理用定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.output.password".<br/>
     * This object is used to specify the GridDB password for output processing.
     * </div>
     */
    GS_OUTPUT_PASSWORD("gs.output.password", null, "password", GS_PASSWORD) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            return value.length() > 0;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },

    /**
     * <div lang="ja">
     * プロパティgs.consistencyに対応した定数です。<br/>
     * GridDBの一貫性レベルを指定するための入出力処理共用定数です。値は"IMMEDIATE"か"EVENTUAL"のどちらかでなくてはなりません。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.consistency".<br/>
     * This object is used to specify the GridDB consistency level for input and/or output processing.<br/>
     * The value must be either "IMMEDIATE" or "EVENTUAL".
     * </div>
     */
    GS_CONSISTENCY("gs.consistency", "", null, null) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            return value.equalsIgnoreCase("IMMEDIATE")
                    || value.equalsIgnoreCase("EVENTUAL");
        }

        @Override
        public String normalize(String value) {
            return value.trim().toUpperCase();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.input.consistencyに対応した定数です。<br/>
     * GridDBの一貫性レベルを指定するための入力処理用定数です。値は"IMMEDIATE"か"EVENTUAL"のどちらかでなくてはなりません。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.input.consistency".<br/>
     * This object is used to specify the GridDB consistency level for input processing.<br/> 
     * The value must be either "IMMEDIATE" or "EVENTUAL".
     * </div>
     */
    GS_INPUT_CONSISTENCY("gs.input.consistency", "", "consistency", GS_CONSISTENCY) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            return value.equalsIgnoreCase("IMMEDIATE")
                    || value.equalsIgnoreCase("EVENTUAL");
        }

        @Override
        public String normalize(String value) {
            return value.trim().toUpperCase();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.output.consistencyに対応した定数です。<br/>
     * GridDBの一貫性レベルを指定するための出力処理用定数です。値は"IMMEDIATE"か"EVENTUAL"のどちらかでなくてはなりません。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.output.consistency".<br/>
     * This object to specify the GridDB consistency level for output processing.<br/>
     * The value must be either "IMMEDIATE" or "EVENTUAL".
     * </div>
     */
    GS_OUTPUT_CONSISTENCY("gs.output.consistency", "", "consistency", GS_CONSISTENCY) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            return value.equalsIgnoreCase("IMMEDIATE")
                    || value.equalsIgnoreCase("EVENTUAL");
        }

        @Override
        public String normalize(String value) {
            return value.trim().toUpperCase();
        }
    },

    /**
     * <div lang="ja">
     * プロパティgs.transaction.timeoutに対応した定数です。<br/>
     * GridDBのトランザクションタイムアウト時間を指定するための入出力処理共用定数です。単位は秒数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.transaction.timeout".<br/>
     * This object is used to specify the transaction timeout time of GridDB for input and/or output processing.<br/>
     * The unit of the value is seconds.
     * </div>
     */
    GS_TRANSACTION_TIMEOUT("gs.transaction.timeout", "", null, null) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            try {
                return Integer.valueOf(value) >= 0;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.input.transaction.timeoutに対応した定数です。<br/>
     * GridDBのトランザクションタイムアウト時間を指定するための入力処理用定数です。単位は秒数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.input.transaction.timeout".<br/>
     * This object is used to specify the transaction timeout time of GridDB for input processing.<br/>
     * The unit of the value is seconds.
     * </div>
     */
    GS_INPUT_TRANSACTION_TIMEOUT("gs.input.transaction.timeout", "", "transactionTimeout", GS_TRANSACTION_TIMEOUT) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            try {
                return Integer.valueOf(value) >= 0;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.output.transaction.timeoutに対応した定数です。<br/>
     * GridDBのトランザクションタイムアウト時間を指定するための出力処理用定数です。単位は秒数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.output.transaction.timeout".<br/>
     * This object is used to specify the transaction timeout time of GridDB for output processing.<br/>
     * The unit of the value is seconds.
     * </div>
     */
    GS_OUTPUT_TRANSACTION_TIMEOUT("gs.output.transaction.timeout", "", "transactionTimeout", GS_TRANSACTION_TIMEOUT) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            try {
                return Integer.valueOf(value) >= 0;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },

    /**
     * <div lang="ja">
     * プロパティgs.failoverに対応した定数です。<br/>
     * GridDBフェイルオーバー待機時間の最低値を指定するための入出力処理共用定数です。単位は秒数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.failover".<br/>
     * This object is used to specify the minimum value of the GridDB failover wait time for input and/or output processing.<br/>
     * The unit of the value is seconds.
     * </div>
     */
    GS_FAILOVER_TIMEOUT("gs.failover.timeout", "", null, null) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            try {
                return Integer.valueOf(value) >= 0;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.input.failoverに対応した定数です。<br/>
     * GridDBフェイルオーバー待機時間の最低値を指定するための入力処理用定数です。単位は秒数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.input.failover".<br/>
     * This object is used to specify the minimum value of the GridDB failover wait time for input processing.<br/>
     * The unit of the value is seconds.
     * </div>
     */
    GS_INPUT_FAILOVER_TIMEOUT("gs.input.failover.timeout", "", "failoverTimeout", GS_FAILOVER_TIMEOUT) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            try {
                return Integer.valueOf(value) >= 0;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.output.failoverに対応した定数です。<br/>
     * GridDBフェイルオーバー待機時間の最低値を指定するための出力処理用定数です。単位は秒数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.output.failover".<br/>
     * This object is used to specify the minimum value of the GridDB failover wait time for output processing.<br/>
     * The unit of the value is seconds.
     * </div>
     */
    GS_OUTPUT_FAILOVER_TIMEOUT("gs.output.failover.timeout", "", "failoverTimeout", GS_FAILOVER_TIMEOUT) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            try {
                return Integer.valueOf(value) >= 0;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },

    /**
     * <div lang="ja">
     * プロパティgs.container.cache.sizeに対応した定数です。<br/>
     * GridDBのコンテナキャッシュの大きさを指定するための入出力処理共用定数です。単位はコ ンテナ数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.container.cache.size".<br/>
     * This object is used to specify the size of the GridDB container cache for input and/or output processing.<br/>
     * The unit of the value is number of containers.
     * </div>
     */
    GS_CONTAINER_CACHE_SIZE("gs.container.cache.size", "", null, null) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            try {
                return Integer.valueOf(value) >= 0;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.input.container.cache.sizeに対応した定数です。<br/>
     * GridDBのコンテナキャッシュの大きさを指定するための入力処理用定数です。単位はコンテ ナ数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.input.container.cache.size".<br/>
     * This object is used to specify the size of the GridDB container cache for input processing.<br/>
     * The unit of the value is number of containers.
     * </div>
     */
    GS_INPUT_CONTAINER_CACHE_SIZE("gs.input.container.cache.size", "", "containerCacheSize", GS_CONTAINER_CACHE_SIZE) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            try {
                return Integer.valueOf(value) >= 0;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.output.container.cache.sizeに対応した定数です。<br/>
     * GridDBのコンテナキャッシュの大きさを指定するための出力処理用定数です。単位はコンテ ナ数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.output.container.cache.size".<br/>
     * This object is used to specify the size of the GridDB container cache for output processing.<br/>
     * The unit of the value is number of containers.
     * </div>
     */
    GS_OUTPUT_CONTAINER_CACHE_SIZE("gs.output.container.cache.size", "", "containerCacheSize", GS_CONTAINER_CACHE_SIZE) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            try {
                return Integer.valueOf(value) >= 0;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },

    /**
     * <div lang="ja">
     * プロパティgs.output.container.nameに対応した定数です。<br/>
     * 出力先のコンテナ名を指定するための定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.output.container.name".<br/>
     * This object is used to specify the container name of the output destination.
     * </div>
     */
    GS_OUTPUT_CONTAINER_NAME("gs.output.container.name", "", null, null) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            return value.length() > 0;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },

    /**
     * <div lang="ja">
     * プロパティgs.input.container.name.listに対応した定数です。<br/>
     * 入力用コンテナ名を指定するための定数です。複数のコンテナ名を指定できます。複数指定 するためのときには','で区切ります。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.input.container.name.list".<br/>
     * This object is used to specify the input container name(s). Multiple container names can be specified by this constant.<br/>
     * When specifying multiple names, use the comma character(',') to delimit those names.
     * </div>
     */
    GS_INPUT_CONTAINER_NAME_LIST("gs.input.container.name.list", "", null, null) {
        @Override
        public boolean validate(String value) {
            return true;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.input.container.name.regexに対応した定数です。<br/>
     * 入力用コンテナ名を正規表現により指定するための定数です。正規表現の書式はjava.util.regex.Patternに従います。
     * 指定した正規表現と照合可能な名前のコンテナが入力対象となります。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.input.container.name.regex".<br/>
     * This object is used to specify the input container name(s) using a regular expression.<br/>
     * The format of a regular expression follows java.util.regex.Pattern.
     * </div>
     */
    GS_INPUT_CONTAINER_NAME_REGEX("gs.input.container.name.regex", "", null, null) {
        @Override
        public boolean validate(String value) {
            return true;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },

    /**
     * <div lang="ja">
     * プロパティgs.input.tql.whereに対応した定数です。<br/>
     * 入力データ選別用のTQL条件式を指定するための定数です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.input.tql.where".<br/>
     * This object is used to specify a conditional expression of TQL for input data selection.
     * </div>
     */
    GS_INPUT_TQL_WHERE("gs.input.tql.where", "", null, null) {
        @Override
        public boolean validate(String value) {
            return true;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },

    /**
     * <div lang="ja">
     * プロパティgs.input.fetch.containersに対応した定数です。<br/>
     * 一度に入力処理する最大コンテナ数を指定するための定数です。デフォルト値は1で、コンテナ毎に入力処理を行います。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.input.fetch.containers".<br/>
     * This object is used to specify the maximum number of containers to be read at a time and the default value of this object is 1.
     * </div>
     */
    GS_INPUT_FETCH_CONTAINERS("gs.input.fetch.containers", "1", null, null) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            try {
                return Integer.valueOf(value) >= 0;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.input.fetch.sizeに対応した定数です。<br/>
     * コンテナ当たりに一括して入力する最大ロウ数を指定するための定数です。デフォルト値は0で、全ロウデータを一括入力します。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.input.fetch.size".<br/>
     * This object is used to specify the maximum number of rows per container to be read at a time.<br/>
     * All rows is read at once with the default value of 0.
     * </div>
     */
    GS_INPUT_FETCH_SIZE("gs.input.fetch.size", "0", null, null) {
        @Override
        public boolean validate(String value) {
            try {
                Integer.valueOf(value);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    /**
     * <div lang="ja">
     * プロパティgs.output.row.buffer.sizeに対応した定数です。<br/>
     * 一括書き込み用バッファのサイズを指定するための定数です。単位はロウ数です。デフォル ト値は1000です。
     * </div><div lang="en">
     * Constant corresponding to the property "gs.output.row.buffer.size".<br/>
     * This object is used to specify the buffer size for writing rows.<br/>
     * The unit of the value is number of rows. The default value of this object is 1000.
     * </div>
     */
    GS_OUTPUT_ROW_BUFFER_SIZE("gs.output.row.buffer.size", "1000", null, null) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            try {
                return Integer.valueOf(value) >= 1;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },

    GS_INPUT_ROW_KEY_ASSIGNED("gs.input.row.key.assigned", null, null, null) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            return value.equalsIgnoreCase("true")
                    || value.equalsIgnoreCase("false");
        }

        @Override
        public String normalize(String value) {
            return value.trim().toLowerCase();
        }
    },
    GS_INPUT_COLUMN_TYPE_LIST("gs.input.column.type.list", null, null, null) {
        @Override
        public boolean validate(String value) {
            return true;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    GS_INPUT_COLUMN_NAME_LIST("gs.input.column.name.list", null, null, null) {
        @Override
        public boolean validate(String value) {
            return true;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    GS_OUTPUT_ROW_KEY_ASSIGNED("gs.output.row.key.assigned", null, null, null) {
        @Override
        public boolean validate(String value) {
            if (value == null) {
                return false;
            }
            return value.equalsIgnoreCase("true")
                    || value.equalsIgnoreCase("false");
        }

        @Override
        public String normalize(String value) {
            return value.trim().toLowerCase();
        }
    },
    GS_OUTPUT_COLUMN_TYPE_LIST("gs.output.column.type.list", null, null, null) {
        @Override
        public boolean validate(String value) {
            return true;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    },
    GS_OUTPUT_COLUMN_NAME_LIST("gs.output.column.name.list", null, null, null) {
        @Override
        public boolean validate(String value) {
            return true;
        }

        @Override
        public String normalize(String value) {
            return value.trim();
        }
    }
    ;

    private final String key_;

    private final String defaultValue_;

    private final String gsPropertyKey_;

    private final GDProperty alias_;

    /**
     * <div lang="ja">
     * コンストラクタ
     * @param key GridDBコネクタ用プロパティの名前
     * @param defaultValue デフォルト値
     * @param gsPropertyKey GridDBのJavaクライアント用プロパティの名前
     * @param alias 入力用、あるいは、出力用定数のときに、対応する入出力処理共用の定数
     * </div><div lang="en">
     * constructor
     * @param key name of GridDB connector property
     * @param defaultValue default value of the property
     * @param gsPropertyKey name of GridDB Java client property
     * @param alias alias object
     * </div>
     */
    private GDProperty(String key, String defaultValue, String gsPropertyKey, GDProperty alias) {
        key_ = key;
        defaultValue_ = defaultValue;
        gsPropertyKey_ = gsPropertyKey;
        alias_ = alias;
    }

    /**
     * <div lang="ja">
     * 定数に対応するGridDBコネクタ用プロパティの名前を返します。
     * @return プロパティ名
     * </div><div lang="en">
     * Return the name of the property used for the GridDB connector corresponding to the constant.
     * @return property name of the GridDB connector property
     * </div>
     */
    public String getKey() {
        return key_;
    }

    /**
     * <div lang="ja">
     * 定数に対応するGridDBコネクタ用プロパティの値を返します。そのプロパティに値が設定さ れていない場合は、以下の優先順位で値を探索します。<br/>
     * 対応する入出力処理共用の定数のプロパティ値<br/>
     * デフォルト値
     * @param conf Configurationオブジェクト
     * @return プロパティ値
     * @throws GDPropertyException 必須プロパティに値が設定されていないか、プロパティの値 が適切でない
     * </div><div lang="en">
     * Return the value of the property used for the GridDB connector corresponding to the constant.
     * If the value has not been set in the property, the value is acquired in the following priority order.<br/>
     * Value of the alias object<br/>
     * Default value if it has been defined
     * @param conf Configuration object
     * @return property　value
     * @throws GDPropertyException either the value cannot be acquired or the value is not appropriate
     * </div>
     */
    public String getProperty(Configuration conf) {
        String property = conf.get(key_, null);

        if (property == null) {
            if (alias_ != null) {
                property = alias_.getProperty(conf);
                if (property != null && property.length() == 0) {
                    property = null;
                }
            }
            if (property == null) {
                if (defaultValue_ != null) {
                    return defaultValue_;
                }
                throw new GDPropertyException("Not Found(" + key_ + ")");
            }
        }

        property = normalize(property);

        if (! validate(property)) {
            throw new GDPropertyException("Illegal Value(" + key_ + "=" + property + ")");
        }

        return property;
    }

    /**
     * <div lang="ja">
     * 定数に対応するGridDBコネクタ用プロパティの値を返します 。そのプロパティに値が設定されていない場合は、以下の優先順位で値を探索します。値が設定されていなければnullを返します。<br/>
     * 対応する入出力処理共用の定数のプロパティ値<br/>
     * デフォルト値
     * @param conf Configurationオブジェクト
     * @return プロパティ値
     * @throws GDPropertyException 必須プロパティに値が設定されていないか、プロパティの値が適切でない
     * </div><div lang="en">
     * Return the value of the property used for the GridDB connector corresponding to the constant.
     * If the value has not been set in the property, the value is acquired in the following priority order.
     * Return null if the length of the string value is 0.<br/>
     * Value of the alias object<br/>
     * Default value if it has been defined
     * @param conf Configuration object
     * @return property value
     * @throws GDPropertyException either the value cannot be acquired or the value is not appropriate
     * </div>
     */
    public String get(Configuration conf) {
        String value = getProperty(conf);
        if (value == null) {
            return null;
        }
        value = value.trim();
        if (value.length() == 0) {
            return null;
        }
        return value;
    }

    /**
     * <div lang="ja">
     * 定数に対応するGridDBのJavaクライアント用プロパティの名前を返します。対応するGridDB のJavaクライアント用プロパティが対応づけられていない場合はnullを返します。
     * @return GridDBのJavaクライアント用プロパティの名前
     * </div><div lang="en">
     * Return the name of the property used for GridDB Java client or null if the Java client property has not been assigned.
     * @return name of GridDB Java client property
     * </div>
     */
    public String getGSPropertyKey() {
        return gsPropertyKey_;
    }

    /**
     * <div lang="ja">
     * 入出力処理共用の定数を返します。対応する入出力処理共用の定数がない場合はnullを返し ます。
     * @return GDPropertyオブジェクト
     * </div><div lang="en">
     * Return the alias object or null if there is no corresponding alias object.
     * @return GDProperty object
     * </div>
     */
    public GDProperty getAlias() {
        return alias_;
    }

    /**
     * <div lang="ja">
     * 指定された値を対応するプロパティに設定します。
     * @param value プロパティ値
     * @param conf Configurationオブジェクト
     * @throws GDPropertyException 指定された値が適切ではない
     * </div><div lang="en">
     * Set value to the GridDB connector property.
     * @param value property value
     * @param conf Configuration object
     * @throws GDPropertyException property value is not appropriate
     * </div>
     */
    public void set(String value, Configuration conf) {
        if (! validate(value)) {
            throw new GDPropertyException("Illegal Value(" + key_ + "=" + value + ")");
        }

        conf.set(getKey(), normalize(value));
    }

    /**
     * <div lang="ja">
     * プロパティ値の書式を検証します。
     * @param value プロパティ値
     * @return 適切な書式であればtrue、そうでなければfalse
     * </div><div lang="en">
     * Check if the format of the property value is appropriate or not.
     * @param value property value
     * @return the format is appropriate or not.
     * </div>
     */
    public abstract boolean validate(String value);

    /**
     * <div lang="ja">
     * プロパティ値の表記を正規化します。
     * @param value プロパティ値
     * @return 正規化した表記
     * </div><div lang="en">
     * Normalize the notation of the property value.
     * @param value property value
     * @return normalized notation
     * </div>
     */
    public abstract String normalize(String value);
}
