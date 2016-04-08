format: hstop
	hdfs namenode -format

hstart: format
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver

hstop: 
	mr-jobhistory-daemon.sh stop historyserver
	stop-yarn.sh
	stop-dfs.sh

neel: hstart
	hadoop fs -mkdir -p /input
	hadoop fs -put all/* /input

run: neel
	hadoop com.sun.tools.javac.Main Main.java
	jar -cf assign5.jar Main*.class
	hadoop jar assign5.jar Main /input /output
	hadoop fs -get /output .

