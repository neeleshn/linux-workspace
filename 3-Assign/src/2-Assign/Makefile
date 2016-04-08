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


