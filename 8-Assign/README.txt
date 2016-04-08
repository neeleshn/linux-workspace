
REQUIRED SOFTWARE, PACKAGES & SETTINGS
=======================================

1. Software
------------- 
 * Oracle JDK version 1.7 or Open JDK 7
 * configured aws-cli
 * sbt

2. Settings
-------------
 * Replace *** with your aws credentials in WebServer.java and WebClient.java
 * Replace the <bucket-name> in Makefile to your bucket name 
 * Copy your key pair .pem file in server directory.
 * Change security group id and key pair name in start-cluster.sh


HOW TO EXECUTE THE PROGRAM
=========================================
1. "make all" builds the project
2. "make start2" or "make start8" to start 2 or 8 instances
3. "time make sort" to run the client program and get its execution time
4. "make stop" to stop the instances

Find the output in topten.txt
