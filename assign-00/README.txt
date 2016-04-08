				Assignment 00 -  Sequential Analysis

Instructions to run:
	
	Requirements:
		openscv-3.6.jar		(included in tarball.gunzip)
		jdk 1.7
		Eclipse IDE

	Steps:
		1.	"Import" the project given in the package into Eclipse

		2.	Place data file "323.csv.gz" in "one-month" folder
			[NOTE: DATAFILE NOT INCLUDED]
		
		-	Ensure the following files are present before proceeding
			\one-month\323.csv.gz
			\src\vedant\Assg00Main.java
			\vedant\sequential\CsvGzReader.java
			\lib\openscv-3.6.jar
				
		3.	Configure build path of this project to add jar "openscv-3.6.jar"
			(Located in "lib" folder)
		
		4.	Clean and build the project, and run "Assg00Main.java" as a java application
		
NOTES:
-	I am ignoring the "header" line in the data file.
	So my output will show K = 4082 and F = 435940
	
-	The mean prices are calculated by taking an average over AVG_TICKET_PRICE values for
	each record for that carrier.
	The list is then sorted and displayed by increasing price.

-	Following sanity checks have been implemented:
		CRSArrTime and CRSDepTime should not be zero
		timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime;
		timeZone % 60 should be 0
		AirportID,  AirportSeqID, CityMarketID, StateFips, Wac should be larger than 0
		Origin, Destination,  CityName, State, StateName should not be empty
		For flights that not Cancelled:
		ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
		if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
		if ArrDelay < 0 then ArrDelayMinutes should be zero
		if ArrDelayMinutes >= 15 then ArrDel15 should be false

Program Output:

4082
435940
WN 59.501335
HP 67.05975
AS 75.470856
PI 190.54068
CO 204.74582
EA 273.3297
PA 284.24783
US 289.21008
TW 298.96524
AA 307.33508
DL 348.12643
UA 547.0867
NW 54354.395
