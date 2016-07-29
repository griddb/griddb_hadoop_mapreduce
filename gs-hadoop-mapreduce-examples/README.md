Sample program of GridDB connector

## Overview

The sample program below can be executed using a GridDB connector in exec-example.sh
.

    wordcount:  Counts the number of occurrences of a term in a text file
                

Run process according to the procedure below in exec-example.sh.

    (1) Create the specified number of containers serving as MapReduce I/O in GridDB
    (2) Import the contents of the specified text file (multiple files can be specified) into the input container(s) on a row basis
    (3) Run a wordcount MapReduce job and store the results in the output container
    (4) Read the execution results stored in the output container and print the results to standard output
    (5) Delete the containers created for sample program

The names of the input container and output container are as follows.

    Input container:  input_1 to input_N (default value of N is 2, and this can be changed in the argument)
    Output container: output

The connection information of GridDB below can be specified in the argument.

    --define notificationAddress=<notification address> optional(default is 239.0.0.1)
    --define notificationPort=<notification port>       optional(default is 31999)
    --define clusterName=<cluster name>                 required
    --define user=<user name>                           required
    --define password=<password>                        required

## wordcount

Specifications of exec-example.sh to run wordcount are as follows.

    ./exec-example.sh   -job wordcount
            [--num-containers <number of containers>]
            --define <key=value> ...
            <filename> ...

    Tally the number of occurrences of the word in the text file specified in <filename>.
