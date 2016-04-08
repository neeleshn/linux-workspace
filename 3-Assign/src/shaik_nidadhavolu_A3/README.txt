------------------------------------
Authors: Habiba Shaik, Neelesh Nidadhavolu
------------------------------------

REQUIRED SOFTWARE, PACKAGES & SETTINGS
=======================================

1. Software
------------- 
 * Oracle JDK version 1.7 or Open JDK 7
 * Hadoop version 2.6.3
 * AWS Command Line Interface and s3cmd commandline
 * R, RStudio and all packages listed in 'R Configuration' section
 * wkhtmltopdf for converting html to pdf

2. Environment variables
-------------------------
Make sure that $HADOOP_HOME, $JAVA_HOME and $HADOOP_CLASSPATH are set.

3. AWS Configuration
---------------------
Please configure authentication keys using 'aws configure' command. Set the region as 'us-east-1a'.

4. R Configuration
--------------------
R code is embedded in the R markdown file `Report.Rmd` in the provided .tar.gz file. 

To successfully execute the script, make sure that the following R packages are installed -
 * ggplot2
 * rmarkdown

RStudio is convenient to work with R scripts. In RStudio console, use the following commands to install the above packages -
	install.packages("ggplot2")
	install.packages("rmarkdown")

HOW TO EXECUTE THE PROGRAM
===========================
**NOTE:: Implemented Map reduce program in Scala for Comparison and also included this in the plot from R.

1. Extract Shaik_Nidadhavolu_A3.tar.gz to get a folder named Shaik_Nidadhavolu_A3.
2. Navigate inside the folder.
3. Make a directory named input inside the main directory and copy contents of "all" directory inside Shaik_Nidadhavolu_A2/input/input1 and some files into /input2/
4.create a folder Shaik_Nidadhavolu_A2/output/
  There should be a s3 folder created as s3://neel-habiba/ which contains input1 and input2 folders with gz files.
5. To run the shell script - 
   * ./benchmark-script 
6. All outputs from program will be saved in Shaik_Nidadhavolu_A2/output/
7. Report will be generated in the same folder as script as Report.pdf




