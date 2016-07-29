Hadoop MapReduce用GridDBコネクタ

## 概要

Hadoop Mapreduce用GridDBコネクタは、GridDBをHadoop MapReduceタスクの入
力源、および、出力先として使用するためのJavaライブラリです。本ライブラ
リによりMapReduceジョブが、インメモリ処理によるGridDBの性能を直接利用で
きるようになります。

## 動作環境

以下の環境でライブラリのビルドとサンプルプログラムの実行を確認しています。

    OS:         CentOS6.7(x64)
    Java:       JDK 1.8.0_60
    Maven:      apache-maven-3.3.9
    Hadoop:     CDH5.7.1(YARN)

## クイックスタート
### 準備

GridDBのJavaクライアントをビルドし、作成したgridstore.jarをlibディレク
トリの下に置きます。

### ビルド


    $ mvn package
を実行し、以下のjarファイルを作成します。

    gs-hadoop-mapreduce-client/target/gs-hadoop-maprduce-client-1.0.0.jar
    gs-hadoop-mapreduce-examples/target/gs-hadoop-maprduce-examples-1.0.0.jar

### サンプルプログラムの実行

GridDBを用いたWordCountプログラムを実行するための操作例は以下のとおりで
す。事前にGridDBとHadoop(HDFSとYARN)を起動しておく必要があります。
hadoopコマンドを利用できる等、それらを利用できる環境で以下を実行してく
ださい。

    $ cd gs-hadoop-mapreduce-examples
    $ ./exec-example.sh \
    > --job wordcount \
    > --define notificationAddress=<GridDB notification address(default is 239.0.0.1)> \
    > --define notificationPort=<GridDB notification port(default is 31999)> \
    > --define clusterName=<GridDB cluster name> \
    > --define user=<GridDB user> \
    > --define password=<GriDB password> \
    > pom.xml 2> /dev/null | sort -r

       5        <dependency>
       5        </dependency>
       3        <groupId>org.apache.hadoop</groupId>
       3        <groupId>com.toshiba.mwcloud.gs.hadoop</groupId>
    ...

最初の数字が出現回数で、右側が処理対象として指定されたファイル
(pom.xml)中の単語です。サンプルプログラムの内容に関しては
gs-hadoop-mapreduce-examples/README_ja.mdを参照ください。

## コミュニティ

  * Issues  
    質問、不具合報告はissue機能をご利用ください。
  * PullRequest  
    GridDB Contributor License Agreement(CLA_rev1.1.pdf)に同意して頂く必要があります。
    PullRequest機能をご利用の場合はGridDB Contributor License Agreementに同意したものとみなします。

## ライセンス

Hadoop MapReduce用GridDBコネクタのライセンスはApache License, version 2.0です。
