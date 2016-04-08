## Author: Habiba Neelesh
#!/bin/bash
echo "Running Script!"

#Pseudo Mode
cd src/pseudo
mr-jobhistory-daemon.sh stop historyserver
stop-yarn.sh
stop-dfs.sh
start-dfs.sh
start-yarn.sh
mr-jobhistory-daemon.sh start historyserver
hadoop fs -rmr /user/temp/output
rm -rf output
hadoop fs -mkdir -p /user/temp
hadoop fs -mkdir -p /user/temp/input1
hadoop fs -put input/ /user/temp/input1
hadoop com.sun.tools.javac.Main Main.java
jar cf main.jar Main*.class
rm -rf output
mkdir output
hadoop jar main.jar Main /user/temp/input1/input /user/temp/output > temp.log
hadoop fs -get /user/temp/output output

cd ../..

########## Plot a Graph #########
Rscript -e "library(rmarkdown); rmarkdown::render('Report-HW4.Rmd')" src/pseudo/output/output 
wkhtmltopdf Report.html Report.pdf





