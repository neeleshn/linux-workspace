## Author: Habiba Neelesh
## ---- outputplot ----
args<-commandArgs(trailingOnly = TRUE)
foldername <- args[1]
#"/Users/ummehabibashaik/Documents/Mapreduce/HW2/Output_Local/input"
setwd(foldername)
files<-list.files()
outputData<-c()
for(i in 1:length(files)){
  if (files[i] == "_SUCCESS" || files[i]=="_temporary"){
    next;
  }
  outputData <- rbind(outputData,read.table(file=files[i], header=FALSE, sep="\t"))
}
colnames(outputData) <- c("Airline","TicketPrice","Month","Count")
uniqueList<-unique(outputData[c(1,4)])
uniqueList<-uniqueList[order(uniqueList$Count,decreasing=TRUE),]
topAirlines <- head(uniqueList[1],10)

outputDataFiltered<-outputData[(outputData$Airline %in% topAirlines$Airline),]
print(outputDataFiltered, col.names = F, row.names = F)
