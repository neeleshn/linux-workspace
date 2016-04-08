#!/bin/bash
# Author: Habiba Neelesh
format: 
	hdfs namenode -format

hstart: 
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver

hstop: 
	mr-jobhistory-daemon.sh stop historyserver
	stop-yarn.sh
	stop-dfs.sh

temp:
	hadoop fs -mkdir -p /user/temp
	hadoop fs -mkdir -p /user/temp/input

jar:
	sbt package
	cp target/scala-*/demo_*.jar Demo.jar

run: jar
	rm -rf out
	sbt run

hadoop1: jar
	rm -rf out
	HADOOP_CLASSPATH=`perl cp.pl` hadoop jar Demo.jar /user/temp/input/input1 /user/temp/output 0


hadoop2: jar
	rm -rf out
	HADOOP_CLASSPATH=`perl cp.pl` hadoop jar Demo.jar /user/temp/input/input1 /user/temp/output 1


hadoop3: jar
	rm -rf out
	HADOOP_CLASSPATH=`perl cp.pl` hadoop jar Demo.jar /user/temp/input/input1 /user/temp/output 2


hadoop4: jar
	rm -rf out
	HADOOP_CLASSPATH=`perl cp.pl` hadoop jar Demo.jar /user/temp/input/input2 /user/temp/output 0


hadoop5: jar
	rm -rf out
	HADOOP_CLASSPATH=`perl cp.pl` hadoop jar Demo.jar /user/temp/input/input2 /user/temp/output 1


hadoop6: jar
	rm -rf out
	HADOOP_CLASSPATH=`perl cp.pl` hadoop jar Demo.jar /user/temp/input/input2 /user/temp/output 2

clean:
	rm -rf project target *.jar out
