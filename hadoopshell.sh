#!/usr/bin/env bash

#git clone https://github.com/CoderYiFei/InformationRetrival.git 
#These are the files you already have:
# StopWordSkipper.java 
# SkipMapper.java
# small
# stopwords

cd ./Hadoop

export HADOOP_CLASSPATH=../../lucene-core-7.2.1.jar:../../lucene-analyzers-common-7.2.1.jar:$HADOOP_CLASSPATH
external_libs="../../lucene-core-7.2.1.jar,../../lucene-analyzers-common-7.2.1.jar"

hdfs dfs -rm -r /xuqing_input
hdfs dfs -rm -r /xuqing_output
hdfs dfs -mkdir /xuqing_input
hdfs dfs -put ../small /xuqing_input

hadoop com.sun.tools.javac.Main *.java
jar cf WC.jar *.class
rm *.class
hadoop jar WC.jar SecondarySortDriver -libjars $external_libs /xuqing_input/small /xuqing_output/temp /xuqing_output/final
cd ../
hdfs dfs -get /xuqing_output/final/*
hdfs dfs -rm -r /xuqing_output
