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

1. Extract Shaik_Nidadhavolu_A4.tar.gz to get a folder named Shaik_Nidadhavolu_A4.
2. Navigate inside the folder.
3. Make a directory named input inside the main directory and copy contents of "all" directory inside Shaik_Nidadhavolu_A4/input
4. There should be an s3 folder created as s3://neel-habiba/ which contains input folder with gz files.
5. Run "Make Regression"
6. Report will be generated in the same folder as script as Report.pdf


