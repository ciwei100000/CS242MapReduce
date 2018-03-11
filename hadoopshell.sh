
#git clone https://github.com/CoderYiFei/InformationRetrival.git 
#These are the files you already have:
# StopWordSkipper.java 
# SkipMapper.java
# small
# stopwords
hdfs dfs -rm -r /input
hdfs dfs -rm -r /output
hdfs dfs -mkdir /input
hdfs dfs -put small /input
hdfs dfs -rm -r /skip
hdfs dfs -mkdir /skip
hdfs dfs -put stopwords /skip
hadoop com.sun.tools.javac.Main *.java
jar cf WC.jar *.class
hadoop jar WC.jar StopWordSkipper -skip /skip/stopwords /input/small /output 
