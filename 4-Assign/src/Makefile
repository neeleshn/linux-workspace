#!/bin/bash
# Author: Nat Tuck

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

neeleshganan:
	hadoop fs -mkdir -p /user/neeleshganan
	hadoop fs -mkdir -p /user/neeleshganan/input

jar:
	sbt package
	cp target/scala-*/demo_*.jar Demo.jar

run: jar
	rm -rf out
	sbt run

hadoop: jar
	rm -rf out
	HADOOP_CLASSPATH=`perl cp.pl` hadoop jar Demo.jar input/all out 0

clean:
	rm -rf project target *.jar out
