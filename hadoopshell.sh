#!/usr/bin/env bash

#git clone https://github.com/CoderYiFei/InformationRetrival.git 
#These are the files you already have:
# StopWordSkipper.java 
# SkipMapper.java
# small
# stopwords

export HADOOP_CLASSPATH=../lib/lucene-core-7.2.1.jar:../lib/lucene-analyzers-common-7.2.1.jar:$HADOOP_CLASSPATH
external_libs="../lib/lucene-core-7.2.1.jar,../lib/lucene-analyzers-common-7.2.1.jar"

hdfs dfs -rm -r /input
hdfs dfs -rm -r /output
hdfs dfs -mkdir /input
hdfs dfs -put TestData /input

hadoop com.sun.tools.javac.Main *.java
jar cf WC.jar *.class
rm *.class
hadoop jar WC.jar StopWordSkipper -libjars $external_libs /input/TestData /output
