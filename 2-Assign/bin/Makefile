psuedo:
	mr-jobhistory-daemon.sh stop historyserver
	stop-yarn.sh
	stop-dfs.sh
	hdfs namenode -format
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver
	hadoop fs -mkdir -p /user/neel
	hadoop fs -mkdir -p /user/neel/input
	hadoop fs -put all/* /user/neel/input
	hadoop com.sun.tools.javac.Main Main.java
	jar cf main.jar Main*.class
	hadoop jar main.jar Main /user/neel/input /user/neel/output
	hadoop fs -get /user/neel/output .
	Rscript -e "rmarkdown::render('HW2Report.Rmd')" output
	
emr:
	hadoop com.sun.tools.javac.Main Main.java
	jar cf Main.jar Main*.class
	aws s3 mb s3://neel-assign2
	aws s3 cp Main.jar s3://neel-assign2/
	aws s3 cp all s3://neel-assign2/input --recursive
	aws emr create-cluster --name "HW2_Cluster" --release-label emr-4.3.0 --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge InstanceGroupType=CORE,InstanceCount=2,InstanceType=m3.xlarge --steps Type=CUSTOM_JAR,Name="Main program JAR",ActionOnFailure=CONTINUE,Jar=s3://neel-assign2/Main.jar,MainClass=Main,Args=[s3://neel-second/input,s3://neel-second/output] --auto-terminate --log-uri s3://neel-second/logs --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-east-1a --enable-debugging
	
