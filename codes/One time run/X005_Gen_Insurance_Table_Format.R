library(XML)
library(downloader)
library(varhandle)
library(gdata)
library(mFilter)
library(Matrix)
library(dplyr)
#library(neverhpfilter)
library(igraph)
library(reshape)
library(reshape2)
library(stringr)
library(DBI)
library(lubridate)
library(RMySQL)
library(zoo)
library(lattice)
library(pracma) # For Moving average
library(plotrix) # For textbox()
library(timeDate)
library(pacman)
p_load(openxlsx)
# library(xlsx)
# require(openx̧̧̧̧lsx)



#********************************----------- DB Deatail -----------------*************************----
Adqvest_Properties = read.delim("C:\\Users\\Administrator\\AdQvestDir\\NIIF_PPT_Properities_file\\AdQvest_properties.txt",
                                 sep = " ", header = T)
                                 # Adqvest_Properties = read.delim("/Users/pushkar/Adqvest/Adqvest_ClickHouse_properties.txt", sep = " ", header = T)
#Adqvest_Properties = read.delim("D:/Adqvest_Office_work/R_Script/AdQvest_properties.txt", sep = " ", header = T)
#Adqvest_Properties = read.delim("C:/Users/jadva/OneDrive/Desktop/Adqvest/error/properties/AdQvest_properties.txt", sep = " ", header = T)
# Adqvest_Properties = read.delim("C:/Users/dell/Dropbox/Adqvest/error/properties/AdQvest_properties.txt", sep = " ", header = T)
#Adqvest_Properties = read.delim("C:/Adqvest/R-workspace/AdQvest_properties.txt", sep = " ", header = T)

username = unlist(strsplit(as.character(Adqvest_Properties[1,2]),':|@'))[1]
password = unlist(strsplit(as.character(Adqvest_Properties[1,2]),':|@'))[2]
hostname = unlist(strsplit(as.character(Adqvest_Properties[1,2]),':|@'))[3]

portnum = as.numeric(as.character(Adqvest_Properties[2,2]))
DBName = as.character(Adqvest_Properties[3,2])
setwd("C:\\Users\\Administrator\\AdQvestDir\\NIIF_PPT")

file_name = paste0("GENERAL_INSURANCE.xlsx")

con  <- dbConnect(RMySQL:::MySQL(), dbname = DBName, host = hostname, port = portnum, user = username, password = password)
query1 = paste0("Select DISTINCT Insurance_Type as insurers from GIC_GENERAL_INSURANCE_MONTHLY_CLEAN_DATA")
ins_type = dbGetQuery(con, query1)$insurers

query = paste0("Select Relevant_Date, Insurers, sum(Gross_Direct_Premium_Income) as Premium  from GIC_GENERAL_INSURANCE_MONTHLY_CLEAN_DATA where  Insurance_Type = '", ins_type[1] ,"' and Relevant_Date >= '2021-01-31' group by Relevant_Date, Insurers order by Insurers ")
data1 = dbGetQuery(con, query)
grid2 <-  acast(data1, Insurers~Relevant_Date, fun.aggregate = sum, value.var = c("Premium"))
col.order <- c(colnames(grid2))
df1 <- data.frame(grid2)
colnames(df1) <- gsub('X', '', colnames(df1))
df1$Insurers <- row.names(df1)
row.names(df1) <- NULL
df1 <- df1[, c(27, 1:26)]
df1 <- as.data.frame(df1)

query = paste0("Select Relevant_Date, Insurers, sum(Gross_Direct_Premium_Income) as Premium  from GIC_GENERAL_INSURANCE_MONTHLY_CLEAN_DATA where  Insurance_Type = '", ins_type[2] ,"' and Relevant_Date >= '2021-01-31' group by Relevant_Date, Insurers order by Insurers ")
data2 = dbGetQuery(con, query)
grid2 <-  acast(data2, Insurers~Relevant_Date, fun.aggregate = sum, value.var = c("Premium"))
col.order <- c(colnames(grid2))
df2 <- data.frame(grid2)
colnames(df2) <- gsub('X', '', colnames(df2))

df2$Insurers <- row.names(df2)
row.names(df2) <- NULL
df2 <- df2[, c(27, 1:26)]
df2 <- as.data.frame(df2)

query = paste0("Select Relevant_Date, Insurers, sum(Gross_Direct_Premium_Income) as Premium  from GIC_GENERAL_INSURANCE_MONTHLY_CLEAN_DATA where  Insurance_Type = '", ins_type[3] ,"' and Relevant_Date >= '2021-01-31' group by Relevant_Date, Insurers order by Insurers ")
data3 = dbGetQuery(con, query)
grid2 <-  acast(data3, Insurers~Relevant_Date, fun.aggregate = sum, value.var = c("Premium"))
col.order <- c(colnames(grid2))
df3 <- data.frame(grid2)
colnames(df3) <- gsub('X', '', colnames(df3))

df3$Insurers <- row.names(df3)
row.names(df3) <- NULL
df3 <- df3[, c(27, 1:26)]
df3 <- as.data.frame(df3)

query = paste0("Select Relevant_Date, Insurers, sum(Gross_Direct_Premium_Income) as Premium  from GIC_GENERAL_INSURANCE_MONTHLY_CLEAN_DATA where  Insurance_Type = '", ins_type[4] ,"' and Relevant_Date >= '2021-01-31' group by Relevant_Date, Insurers order by Insurers ")
data4 = dbGetQuery(con, query)
grid2 <-  acast(data4, Insurers~Relevant_Date, fun.aggregate = sum, value.var = c("Premium"))
col.order <- c(colnames(grid2))
df4 <- data.frame(grid2)
colnames(df4) <- gsub('X', '', colnames(df4))

df4$Insurers <- row.names(df4)
row.names(df4) <- NULL
df4 <- df4[, c(27, 1:26)]
df4 <- as.data.frame(df4)

query = paste0("Select Relevant_Date, Insurers, sum(Gross_Direct_Premium_Income) as Premium  from GIC_GENERAL_INSURANCE_MONTHLY_CLEAN_DATA where  Insurance_Type = '", ins_type[5] ,"' and Relevant_Date >= '2021-01-31' group by Relevant_Date, Insurers order by Insurers ")
data5 = dbGetQuery(con, query)
grid2 <-  acast(data5, Insurers~Relevant_Date, fun.aggregate = sum, value.var = c("Premium"))
col.order <- c(colnames(grid2))
df5 <- data.frame(grid2)
colnames(df5) <- gsub('X', '', colnames(df5))

df5$Insurers <- row.names(df5)
row.names(df5) <- NULL
df5 <- df5[, c(27, 1:26)]
df5 <- as.data.frame(df5)

query = paste0("Select Relevant_Date, Insurers, sum(Gross_Direct_Premium_Income) as Premium  from GIC_GENERAL_INSURANCE_MONTHLY_CLEAN_DATA where  Insurance_Type = '", ins_type[6] ,"' and Relevant_Date >= '2021-01-31' group by Relevant_Date, Insurers order by Insurers ")
data6 = dbGetQuery(con, query)
grid2 <-  acast(data6, Insurers~Relevant_Date, fun.aggregate = sum, value.var = c("Premium"))
col.order <- c(colnames(grid2))
df6 <- data.frame(grid2)
colnames(df6) <- gsub('X', '', colnames(df6))

df6$Insurers <- row.names(df6)
row.names(df6) <- NULL
df6 <- df6[, c(27, 1:26)]
df6 <- as.data.frame(df6)

query = paste0("Select Relevant_Date, Insurers, sum(Gross_Direct_Premium_Income) as Premium  from GIC_GENERAL_INSURANCE_MONTHLY_CLEAN_DATA where  Insurance_Type = '", ins_type[7] ,"' and Relevant_Date >= '2021-01-31' group by Relevant_Date, Insurers order by Insurers ")
data7 = dbGetQuery(con, query)
grid2 <-  acast(data7, Insurers~Relevant_Date, fun.aggregate = sum, value.var = c("Premium"))
col.order <- c(colnames(grid2))
df7 <- data.frame(grid2)
colnames(df7) <- gsub('X', '', colnames(df7))

df7$Insurers <- row.names(df7)
row.names(df7) <- NULL
df7 <- df7[, c(27, 1:26)]
df7 <- as.data.frame(df7)

query = paste0("Select Relevant_Date, Insurers, sum(Gross_Direct_Premium_Income) as Premium  from GIC_GENERAL_INSURANCE_MONTHLY_CLEAN_DATA where  Insurance_Type = '", ins_type[8] ,"' and Relevant_Date >= '2021-01-31' group by Relevant_Date, Insurers order by Insurers ")
data8 = dbGetQuery(con, query)
grid2 <-  acast(data8, Insurers~Relevant_Date, fun.aggregate = sum, value.var = c("Premium"))
col.order <- c(colnames(grid2))
df8 <- data.frame(grid2)
colnames(df8) <- gsub('X', '', colnames(df8))

df8$Insurers <- row.names(df8)
row.names(df8) <- NULL
df8 <- df8[, c(27, 1:26)]
df8 <- as.data.frame(df8)

query = paste0("Select Relevant_Date, Insurers, sum(Gross_Direct_Premium_Income) as Premium  from GIC_GENERAL_INSURANCE_MONTHLY_CLEAN_DATA where  Insurance_Type = '", ins_type[9] ,"' and Relevant_Date >= '2021-01-31' group by Relevant_Date, Insurers order by Insurers ")
data9 = dbGetQuery(con, query)
grid2 <-  acast(data9, Insurers~Relevant_Date, fun.aggregate = sum, value.var = c("Premium"))
col.order <- c(colnames(grid2))
df9 <- data.frame(grid2)
colnames(df9) <- gsub('X', '', colnames(df9))

df9$Insurers <- row.names(df9)
row.names(df9) <- NULL
df9 <- df9[, c(27, 1:26)]
df9 <- as.data.frame(df9)

query = paste0("Select Relevant_Date, Insurers, sum(Gross_Direct_Premium_Income) as Premium  from GIC_GENERAL_INSURANCE_MONTHLY_CLEAN_DATA where  Insurance_Type = '", ins_type[10] ,"' and Relevant_Date >= '2021-01-31' group by Relevant_Date, Insurers order by Insurers ")
data10 = dbGetQuery(con, query)
grid2 <-  acast(data10, Insurers~Relevant_Date, fun.aggregate = sum, value.var = c("Premium"))
col.order <- c(colnames(grid2))
df10 <- data.frame(grid2)
colnames(df10) <- gsub('X', '', colnames(df10))

df10$Insurers <- row.names(df10)
row.names(df10) <- NULL
df10 <- df10[, c(27, 1:26)]
df10 <- as.data.frame(df10)

query = paste0("Select Relevant_Date, Insurers, sum(Gross_Direct_Premium_Income) as Premium  from GIC_GENERAL_INSURANCE_MONTHLY_CLEAN_DATA where  Insurance_Type = '", ins_type[11] ,"' and Relevant_Date >= '2021-01-31' group by Relevant_Date, Insurers order by Insurers ")
data11 = dbGetQuery(con, query)
grid2 <-  acast(data11, Insurers~Relevant_Date, fun.aggregate = sum, value.var = c("Premium"))
col.order <- c(colnames(grid2))
df11 <- data.frame(grid2)
colnames(df11) <- gsub('X', '', colnames(df11))

df11$Insurers <- row.names(df11)
row.names(df11) <- NULL
df11 <- df11[, c(27, 1:26)]
df11 <- as.data.frame(df11)

query = paste0("Select Relevant_Date, Insurers, sum(Gross_Direct_Premium_Income) as Premium  from GIC_GENERAL_INSURANCE_MONTHLY_CLEAN_DATA where  Insurance_Type = '", ins_type[12] ,"' and Relevant_Date >= '2021-01-31' group by Relevant_Date, Insurers order by Insurers ")
data12 = dbGetQuery(con, query)
grid2 <-  acast(data12, Insurers~Relevant_Date, fun.aggregate = sum, value.var = c("Premium"))
col.order <- c(colnames(grid2))
df12 <- data.frame(grid2)
colnames(df12) <- gsub('X', '', colnames(df12))

df12$Insurers <- row.names(df12)
row.names(df12) <- NULL
df12 <- df12[, c(27, 1:26)]
df12 <- as.data.frame(df12)

query = paste0("Select Relevant_Date, Insurers, sum(Gross_Direct_Premium_Income) as Premium  from GIC_GENERAL_INSURANCE_MONTHLY_CLEAN_DATA where  Insurance_Type = '", ins_type[13] ,"' and Relevant_Date >= '2021-01-31' group by Relevant_Date, Insurers order by Insurers ")
data13 = dbGetQuery(con, query)
grid2 <-  acast(data13, Insurers~Relevant_Date, fun.aggregate = sum, value.var = c("Premium"))
col.order <- c(colnames(grid2))
df13 <- data.frame(grid2)
colnames(df13) <- gsub('X', '', colnames(df13))

df13$Insurers <- row.names(df13)
row.names(df13) <- NULL
df13 <- df13[, c(27, 1:14)]
df13 <- as.data.frame(df1)

query = paste0("Select Relevant_Date, Insurers, sum(Gross_Direct_Premium_Income) as Premium  from GIC_GENERAL_INSURANCE_MONTHLY_CLEAN_DATA where  Insurance_Type = '", ins_type[14] ,"' and Relevant_Date >= '2021-01-31' group by Relevant_Date, Insurers order by Insurers ")
data14 = dbGetQuery(con, query)
grid2 <-  acast(data14, Insurers~Relevant_Date, fun.aggregate = sum, value.var = c("Premium"))
col.order <- c(colnames(grid2))
df14 <- data.frame(grid2)
colnames(df14) <- gsub('X', '', colnames(df14))

df14$Insurers <- row.names(df14)
row.names(df14) <- NULL
df14 <- df14[, c(27, 1:26)]
df14 <- as.data.frame(df14)

query = paste0("Select Relevant_Date, Insurers, sum(Gross_Direct_Premium_Income) as Premium  from GIC_GENERAL_INSURANCE_MONTHLY_CLEAN_DATA where  Insurance_Type = '", ins_type[15] ,"' and Relevant_Date >= '2021-01-31' group by Relevant_Date, Insurers order by Insurers ")
data15 = dbGetQuery(con, query)
grid2 <-  acast(data15, Insurers~Relevant_Date, fun.aggregate = sum, value.var = c("Premium"))
col.order <- c(colnames(grid2))
df15 <- data.frame(grid2)
colnames(df15) <- gsub('X', '', colnames(df15))
df15$Insurers <- row.names(df15)
row.names(df15) <- NULL
df15 <- df15[, c(27, 1:26)]
df15 <- as.data.frame(df15)

query = paste0("Select Relevant_Date, Insurers, sum(Gross_Direct_Premium_Income) as Premium  from GIC_GENERAL_INSURANCE_MONTHLY_CLEAN_DATA where  Insurance_Type = '", ins_type[16] ,"' and Relevant_Date >= '2021-01-31' group by Relevant_Date, Insurers order by Insurers ")
data16 = dbGetQuery(con, query)
grid2 <-  acast(data16, Insurers~Relevant_Date, fun.aggregate = sum, value.var = c("Premium"))
col.order <- c(colnames(grid2))
df16 <- data.frame(grid2)
colnames(df16) <- gsub('X', '', colnames(df16))
df16$Insurers <- row.names(df16)
row.names(df16) <- NULL
df16 <- df16[, c(2, 1)]
df16 <- as.data.frame(df16)

dbDisconnect(con)
list_of_datasets <- list('Health-Retail' = df1,
                         'Health-Group' = df2,
                         'Health-Government schemes' = df3,
                         'Overseas Medical' = df4,
                         'Crop Insurance' = df5,
                         'Credit Guarantee' = df6,
                         'All Other miscellaneous' = df7,
                         'Fire' = df8,
                         'Marine Cargo' = df9,
                         'Marine Hull' = df10,
                         'Engineering' = df11,
                         'Aviation' = df12,
                         'Liability' = df13,
                         'P.A.' = df14,
                         'Motor' = df15,
                         'Crop+Credit Guarantee+Others' = df16)


write.xlsx(list_of_datasets, file_name,
           colNames=c(rep(TRUE, length(list_of_datasets))),
           rowNames=c(rep(TRUE, length(list_of_datasets))),
           asTable = c(TRUE),
           firstActiveRow = c(rep(TRUE, length(list_of_datasets))),
           firstActiveCol = c(rep(TRUE, length(list_of_datasets))))



