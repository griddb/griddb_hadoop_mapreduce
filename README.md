GridDB connector for Hadoop MapReduce

## Overview

The Hadoop MapReduce GridDB connector is a Java library for using GridDB as an input source and output destination for Hadoop MapReduce jobs.
 This library allows the GridDB performance to be used directly by MapReduce jobs through in-memory processing.


## Operating environment

Building of the library and execution of the sample programs are checked in the environment below.

    OS:         CentOS6.7(x64)
    Java:       JDK 1.8.0_60
    Maven:      apache-maven-3.3.9
    Hadoop:     CDH5.X(YARN)

## QuickStart
### Preparations

Build a GridDB Java client and place the created gridstore.jar under the lib directory.


### Build

Run the mvn command like the following:
    $ mvn package
and create the following jar files. 

    gs-hadoop-mapreduce-client/target/gs-hadoop-maprduce-client-1.0.0.jar
    gs-hadoop-mapreduce-examples/target/gs-hadoop-maprduce-examples-1.0.0.jar

### Running the sample program

An operating example to run the WordCount program using GridDB is shown below.
 GridDB and Hadoop (HDFS and YARN) need to be started in advance.
Run the following in an environment in which these and hadoop commands can be used.


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

The first number is the number of occurrences while the right side is a word in the file
(pom.xml) specified as a processing target. See 
gs-hadoop-mapreduce-examples/README.md for details about the sample programs.

## Community

  * Issues  
    Use the GitHub issue function if you have any requests, questions, or bug reports. 
  * PullRequest  
    Use the GitHub pull request function if you want to contribute code.
    You'll need to agree GridDB Contributor License Agreement(CLA_rev1.1.pdf).
    By using the GitHub pull request function, you shall be deemed to have agreed to GridDB Contributor License Agreement.

## License
  
The Hadoop MapReduce GridDB connector source license is Apache License, version 2.0.
