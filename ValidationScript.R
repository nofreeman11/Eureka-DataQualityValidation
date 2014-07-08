### Read in Morning Report Data
# Load required packages
library(DBI)
library(Matrix)
library(PivotalR)
library(RODBC)
library(RPostgreSQL)
library(plyr)

# Get Morning Report data pull from fileshare
setwd("//honts5006/BnP/Data/BnPExtract")
MRFile <- "MD_WMDSPExtract_DMR.txt"

# !IMPORTANT!: Add directory of your choosing below to set as myDirectory, report will be generated here 
myDirectory <- "C:/Users/nfreema/Desktop/Eureka/R"
setwd(myDirectory)

# Create variable for morning report column names
MRColNames <- c("1", "Dept", "Type", "Date", "MR_Sales", "Sales_Cost", "Comp_Sales",
                "LY_Comp_Sales", "Service_Income", "Serv_Inc_Cost", "GO_Markdowns",
                "Receipts_Rtl", "Receipts_Cost", "Store_Markdowns", "TWAY_Markdowns_Cost",
                "COOPS", "DEALS", "Receipts_Costacctg_Rtl", "Receipts_Costacctg_Cost",
                "Costacctg_Sales", "SEM_Markdowns", "GO_New_Markdowns", "AD_Markdowns",
                "COMP_Markdowns", "REDUCED_Markdowns", "TWAY_Markdowns", "TLC_Markdowns", "NA")

# Create variable to set column classes and skip certain columns for morning report data
setAs("character","myDate", function(from) as.Date(from, format="%A, %B %d, %Y") )
setClass('myDate')
MRColClasses <- c("NULL", rep("character", 2),  "myDate", "integer", "NULL", rep("integer", 2), rep("NULL", 20))

# Read in morning report data
MRData <- read.table(MRFile, sep="|", skip=1, col.names=MRColNames, colClasses=MRColClasses, nrows=383670)

# Subset morning report data to last 30 days and remove projections
BeginDate <- Sys.Date() - 32
EndDate <- Sys.Date() - 2
MRData <- MRData[which(MRData$Date>=BeginDate & MRData$Date<=EndDate & (MRData$Type!="PROJECTION        ")),]
# MRData <- MRData[which(MRData$Date=='2014-06-10' & (MRData$Type!="PROJECTION        ")),]

# Create column for budget data and remove type column
budget <- MRData$MR_Sales[which(MRData$Type == "BUDGET            ")]
MRData <- MRData[MRData$Type != "BUDGET            ",]
MRData$Type <- NULL
MRData$MR_Budget <- unlist(budget)

# Clean up dept column
MRData$Dept <- sub('DEPT_','',MRData$Dept)
MRData$Dept <- sub('.DIV..','',MRData$Dept)
MRData$Dept <- as.numeric(MRData$Dept)

# Combine like departments
MRData <- ddply(MRData, .(Dept, Date), summarize,
                      MR_Sales = sum(MR_Sales),
                      Comp_Sales = sum(Comp_Sales),
                      LY_Comp_Sales = sum(LY_Comp_Sales),
                      MR_Budget = sum(MR_Budget))


### Read in Eureka data
#Set password
password = 'walmart'

#Connect to Greenplum
cid <- db.connect('gpprod1', 'nfreema', 'db_prod1', password, 5432)

#Import data from Eureka
EurekaData <- db.q("select 
                   when_value_id, 
                   what_value_id,
                   what_value,
                   sales,
                   ly_sales,
                   units,
                   ly_units,
                   mdse_budget_amount,
                   ly_mdse_budget_amount,
                   visit_cnt,
                   ly_visit_cnt
                   from 
                   wm_ad_hoc.eureka_data_perspective_wm_view
                   where where_country = 'US' and where_type = 'Country' and where_value = 'US' and
                   when_parent_year = 2014 and when_type = 'Day' and (when_value_id  >=  (now()::date - 32) AND when_value_id < now()::date - 2)  and
                   who_type = 'TOTAL' and who_value_id = 'TOTAL' and
                   what_type = 'Department' 
                   
                   ORDER BY substring(what_value_id, '[0123456789]+')::int, when_value_id;", nrows = 5000, conn.id = cid, sep = " ", verbose = TRUE)

#Close all connections
closeAllConnections()


#Replace the Dxx column numbers with just the xx column numbers using the substr() command
DeptCol <- EurekaData[ ,"what_value_id"]
TrimmedDeptCol <- cbind(as.numeric(substr(DeptCol, 2, 50)))

#Replace the what_value_id column in Eureka Data with the TrimmedDeptCol 

EurekaData$what_value_id <- TrimmedDeptCol

#Rename the columns in EurekaData
colnames(EurekaData) <- c("Date", "Dept", "Dept_Description",
                          "DC_Sales", "DC_LY_Sales", "DC_Units", "DC_LY_Units", 
                          "DC_Budget", "DC_LY_Budget", 
                          "DC_Visit_Count", "DC_LY_Visit_Count")


# Merge the EurekaData and MRData by department and date.
MergedTable <- merge(MRData, EurekaData, by=c("Dept", "Date"))


# Create a report using the merged Eureka and morning report data, adding comparison metrics
Report <- MergedTable
Report$Sales_Diff <- round(abs(Report$MR_Sales - Report$DC_Sales), digits = 2)
Report$Sales_Percent_Diff <- round(100*(abs(Report$MR_Sales-Report$DC_Sales)/((1/2)*(Report$MR_Sales + Report$DC_Sales))), digits = 2)
Report$Budget_Diff <- round(abs(Report$MR_Budget - Report$DC_Budget), digits = 2)
Report$Budget_Percent_Diff <- round(100*(abs(Report$MR_Budget-Report$DC_Budget)/((1/2)*(Report$MR_Budget + Report$DC_Budget))), digits = 2)

# Clean up report
# drops <- c("Comp_Sales","LY_Comp_Sales","Dept_Description","DC_LY_Sales","DC_Units","DC_LY_Units",
#            "DC_LY_Budget","DC_Visit_Count","DC_LY_Visit_Count","NA")
# Report <- Report[,!(names(Report) %in% drops)]
Report <- Report[ ,c("Date", "Dept", "Dept_Description", "MR_Sales", "DC_Sales", "Sales_Diff", "Sales_Percent_Diff",
                       "MR_Budget", "DC_Budget", "Budget_Diff", "Budget_Percent_Diff")]

# Create a csv file containing the results
# write.csv(Report, file = "DataQualityReport.csv")
# write.csv(Report, file = paste("DataQualityReport", format(Sys.time(), "%Y-%m-%d_%I%M%S_%p"), ".csv", sep = ""))

# Create a dataframe summarized by dept
SummaryData <- ddply(Report, c("Dept"), summarise,
                     Sales_Percent_Diff_Median = median(Sales_Percent_Diff),
                     Sales_Percent_Diff_Mean = mean(Sales_Percent_Diff),
                     Budget_Percent_Diff_Median = median(Budget_Percent_Diff),
                     Budget_Percent_Diff_Mean = mean(Budget_Percent_Diff))

# Create an excel workbook containing the results
library(xlsx)
exportFile <- paste("DataQualityReport", format(Sys.time(), "%Y-%m-%d_%I%M%S_%p"), ".xlsx", sep = "")
write.xlsx(Report, file = exportFile,
           sheetName="30DaysByDept", col.names=TRUE, row.names=TRUE, append=FALSE, showNA=TRUE)
write.xlsx(SummaryData, file = exportFile,
           sheetName="DeptSummary", col.names=TRUE, row.names=TRUE, append=TRUE, showNA=TRUE)

mywb <- loadWorkbook(exportFile)

### Messaround

# histData <- c(summaryData$Sales_Percent_Diff_Mean)
# 
# hist(histData, xlim=c(0,5), breaks=c(1,2,3,4))
