#SetUpConfiguration
logEnabled <- TRUE
options(java.parameters = c("-XX:+UseConcMarkSweepGC", "-Xmx8192m"))
# prepareEnvironment function: install and load multiple R packages.
# check to see if packages are installed. Install them if they are not, then load them into the R session.
prepareEnvr <- function(pkg){
  new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
  if (length(new.pkg)) 
    install.packages(new.pkg, dependencies = TRUE)
  sapply(pkg, require, character.only = TRUE)
}

# Prepare Environment
packages <- c("dplyr","tidyverse","data.table", "stringr","reshape","skimr","readxl", "janitor","logr")
# " "magrittr""," "sqldf"", ""ggplot2"","data.table", "stringr","plyr","reshape","skimr","repr","janitor"[clean names],"sparklyr",
prepareEnvr(packages)

# Set up the timeout
getOption('timeout')
options(timeout=600)

# Create variables for paths of the datasets
path_property <-"http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv"
path_mortgage <- "http://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/Cash-mortgage-sales-2020-05.csv?utm_medium=GOV.UK&utm_source=datadownload&utm_campaign=cash_mortgage-sales&utm_term=9.30_02_09_20"
path_deprivation <- "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/833978/File_5_-_IoD2019_Scores.xlsx"

######################### Data Extraction #####################################################

column_names<-c("price","date","property_type","build_type","tenure","district","county")
property <- fread(path_property, select = c(2,3,5,6,7,13,14),data.table = FALSE, header = FALSE)
setnames(property, column_names)
mortgage <- fread(path_mortgage,select = c(1,2,3,4,8,9,13),data.table = FALSE)
head(mortgage)
xlsFile = "social_deprevation.xlsx"
download.file(url=path_deprivation, destfile=xlsFile, mode="wb")
social_deprivation <- read_excel(xlsFile,2)

######################### Data Preprocessing ############################################### 
###############Property Dataset########################################################
head(property)
property$date <- format(as.Date(property$date,format = "%Y-%m-%d"),"%Y%m")
property <- subset(property, date %in% 201601:201912)
property <- property %>%
  dplyr::rename(
    region_name = 'district'
  )

# Clean the column names
property<-clean_names(property)

# Calculating the average Price
tempProperty <- property
tempProperty$no_of_sales <- ""
tempProperty <- aggregate(no_of_sales~ date + property_type  + build_type  + tenure + region_name + county,
                      data = tempProperty , FUN = length)

property <- aggregate(cbind(price) ~ date + property_type  + build_type  + tenure + region_name + county,
                      data = property, FUN = mean)
property$no_of_sales <- tempProperty$no_of_sales 

dim(property)
head(property)
rm(tempProperty)

property <- property %>%
  dplyr::rename(
    average_price = "price",
  )
head(property)

#make all values in the region name to lower case before data merging
property$region_name <-tolower(property$region_name)

head(property)

####################### Data Validations  ########################################
dataValidationFn <- function(df){
  print("Checking the duplicates....")
  cat('\nThe number of duplicate entries in the data are', sum(duplicated(df)))
  cat("\nValidating the missing values......")
  print(any(is.na(df)))
  cat("\nThe total number of null values in the dataset ", sum(is.na(property)))
  cat('\nValidate rows with missing values') 
  print(rownames(df)[apply(df, 2, anyNA)])
  cat('\nValidate columns with missing values')
  print(colnames(df)[apply(df, 2, anyNA)])
}
dataValidationFn(property)

####################### Data Cleaning  ########################################
cat('The shape of the dataframe is', dim(property))
manageNullValues<- function(df){
  print("\nRemove the empty rows and columns of data...")
  df <- remove_empty(df, which = c("rows","cols"), quiet = FALSE)
  cat('\nAfter removing the null values the shape of the dataframe is', dim(df))
  # Remove missing values from the dataset
  df <- df %>% drop_na()
  cat('\nAfter droping the null values of the dataframe is', dim(df))
  # Return the dataframe after removing null values
  return(df)
}

manageNullValues(property)

manageDuplicate<- function(df){
  # Validate the duplicates in the data
  cat('\nThe number of duplicate entries in the data are', sum(duplicated(df)))
  # Remove duplicates
  df <- df %>% distinct()
  cat('\nAfter removing the duplicates the shape of the dataframe is', dim(df))
  return(df)
}
manageDuplicate(property)

# # Outlier Detection
outliers <- function(x) {
  
  Q1 <- quantile(x, probs=.25)
  Q3 <- quantile(x, probs=.75)
  iqr = Q3-Q1
  
  upper_limit = Q3 + (iqr*1.5)
  lower_limit = Q1 - (iqr*1.5)
  
  x > upper_limit | x < lower_limit
}

remove_outliers <- function(df, cols = names(df)) {
  for (col in cols) {
    df <- df[!outliers(df[[col]]),]
  }
  df
}
remove_outliers(property, c('average_price'))
dim(property)
head(property)

##################################################################
# Mortgage Dataset
mortgage$Date <- format(as.Date(mortgage$Date,format = "%Y-%m-%d"),"%Y%m")
mortgage <- subset(mortgage, Date %in% 201601:201912)

################ Data Cleaning #######################
dim(mortgage)
head(mortgage)

mortgage$Region_Name <- recode(mortgage$Region_Name,
                               'bournemouth, christchurch and poole' = 'bournemouth christchurch and poole',
)

mortgage<-mortgage[mortgage$Mortgage_Average_Price != 0, ]
cat('The shape of the dataframe is', dim(mortgage))

mortgage<-clean_names(mortgage)
head(mortgage)

mortgage$region_name  = tolower(mortgage$region_name)
mortgage$area_code = tolower(mortgage$area_code)

# Remove empty rows and columns of data
cat('The shape of the dataframe is', dim(mortgage))
mortgage <- manageNullValues(mortgage)
mortgage <- manageDuplicate(mortgage)
dim(mortgage)
mortgage <- remove_outliers(mortgage, c('cash_average_price','cash_sales_volume',
                            'mortgage_average_price','mortgage_sales_volume'))
dim(mortgage)

################################################################
#####Social Deprivation###################
head(social_deprivation)
# Remove unwanted columns
social_deprivation <-social_deprivation[ , c(4,5,6,7,10)]

social_deprivation <- social_deprivation %>%
  dplyr::rename(
    region_name = "Local Authority District name (2019)",
    imd_score = "Index of Multiple Deprivation (IMD) Score",
    income_score = "Income Score (rate)",
    employment_score = "Employment Score (rate)",
    crime_score = "Crime Score"
  )
dim(social_deprivation)

# Aggregate data
social_deprivation <- aggregate(social_deprivation,
                                by = list(social_deprivation$region_name),
                                FUN = mean)

social_deprivation[ , c("region_name")] <- list(NULL)
head(social_deprivation)

social_deprivation <- social_deprivation %>%
  dplyr::rename(
    region_name = "Group.1",
  )

dim(social_deprivation)
head(social_deprivation)

# Making standardized Header names and making all region_name to lower
social_deprivation<-clean_names(social_deprivation)
social_deprivation$region_name  = tolower(social_deprivation$region_name)
head(social_deprivation)

dataValidationFn(social_deprivation)
social_deprivation <- manageNullValues(social_deprivation)
social_deprivation <- manageDuplicate(social_deprivation)
remove_outliers(social_deprivation,c(2:5))
dim(social_deprivation)

######################################## Data Merge ############################################## 

df_list <- list(property, mortgage)

merged_data <- Reduce(function(x, y) merge(x, y, by=c("region_name","date"), all.x=TRUE), df_list, accumulate=FALSE)
head(merged_data, n = 5)

dataValidationFn(merged_data)
merged_data <- manageNullValues(merged_data)
merged_data <- manageDuplicate(merged_data)
dim(merged_data)

df_list <- list(merged_data,social_deprivation)
merged_data <- Reduce(function(x, y) merge(x, y, by=c("region_name"), all.x=TRUE), df_list, accumulate=FALSE)
dim(merged_data)
head(merged_data)

dataValidationFn(merged_data)
merged_data <- manageNullValues(merged_data)
merged_data <- manageDuplicate(merged_data)
dim(merged_data)
head(merged_data)


#### Clearing unwanted dataframes created #################
rm(property, mortgage, social_deprivation, path_deprivation,path_mortgage,path_property, xlsFile)
#rm(list=ls())
skim(merged_data)

##################################### Data Loading ########################################################
library(sqldf)
library(DBI)

#Creating connection
con <- dbConnect(odbc::odbc(), .connection_string = "Driver={Cloudera ODBC Driver for Apache Hive};"
                 , timeout = 10,Host = "sandbox-hdp.hortonworks.com",Port =10000)

#Create database
dbGetQuery(con, "CREATE DATABASE IF NOT EXISTS realestate")
table = "use realestate"
dbGetQuery(con,table)


# Function to write tables as chunks to avoid memory heap error
writeTablesToHive <- function(df,id){
  batch_size = 3000 # batchsize
  # cycles through data by batchsize
  for (i in 1:ceiling(nrow(df)/batch_size))
  {
    # below shows how to cycle through data 
    batch <- df[(((i - 1)*batch_size) + 1):(batch_size*i), , drop = FALSE] # drop = FALSE keeps it from being converted to a vector 
    # if below not done then the last batch has Nulls above the number of rows of actual data
    batch <- batch[!is.na(batch$id),] # ID is a variable I presume is in every row
    if(i == 1){
      (dbWriteTable(conn = con, "df", value = batch, append = TRUE,row.names = FALSE))
    }
    else{
      dbAppendTable(conn = con,name ="df",value = batch ,row.names = NULL)
    } 
    Sys.sleep(10) # to give some sleep time
  }
}

########################################### Create Dimensions ########################################

# Create table DimRegion
dbGetQuery(con,"CREATE TABLE IF NOT EXISTS DimRegion(area_code string, region_name string, county string, PRIMARY KEY(area_code) disable novalidate)")
# Create a dataframe for DimRegion
DimRegion <- sqldf("SELECT merged_data.area_code, merged_data.region_name, merged_data.county FROM merged_data
      GROUP BY merged_data.area_code, merged_data.region_name, merged_data.county")
# Write the dataframe to Hive datatable
writeTablesToHive(DimRegion,area_code)


# Create table DimTime
dbGetQuery(con,"CREATE TABLE IF NOT EXISTS DimTime(time_id string, year string, month string, PRIMARY KEY(time_id) disable novalidate)")
# Create dataframe DimTime with select query
DimTime <- sqldf("SELECT merged_data.date AS time_id, SUBSTRING(merged_data.date,1,4) AS Year, SUBSTRING(merged_data.date,5,2) AS month
 FROM merged_data GROUP BY merged_data.date")
#write the dataframe to Hive datatable
writeTablesToHive(DimTime,time_id)


#Create table DimProperty
dbGetQuery(con,"CREATE TABLE IF NOT EXISTS DimProperty(property_Id string, property_type string, build_type string,
           tenure string, PRIMARY KEY(property_Id) disable novalidate)")
DimProperty <- sqldf("SELECT (merged_data.property_type||merged_data.build_type||merged_data.tenure) AS property_Id, merged_data.property_type,
    merged_data.build_type, merged_data.tenure FROM merged_data
    GROUP BY (merged_data.property_type||merged_data.build_type||merged_data.tenure), merged_data.property_type, merged_data.build_type, merged_data.tenure")
writeTablesToHive(DimTime,property_id)


#dbGetQuery(con, "Drop table if exists DimDepression")
# Create DimDepression
dbGetQuery(con,"CREATE TABLE  IF NOT EXISTS DimDepression(depression_id int,imd_score string, income_score string,
    employment_score string, crime_score string, PRIMARY KEY(depression_id) disable novalidate)")
DimDepression <- sqldf("SELECT row_number() over () as depression_id, merged_data.imd_score, merged_data.income_score,
merged_data.employment_score, merged_data.crime_score FROM merged_data GROUP BY
merged_data.imd_score, merged_data.income_score, merged_data.employment_score,
                       merged_data.crime_score")
writeTablesToHive(DimDepression, depression_id)


#Create DimMortage
dbGetQuery(con,"CREATE TABLE  IF NOT EXISTS DimMortage(mortgage_id string, cash_average_price string, cash_sales_volume string,
mortgage_average_price string, mortgage_sales_volume string, primary key(mortgage_id) disable novalidate)")
DimMortage <- sqldf("SELECT merged_data.area_code|| cast(merged_data.cash_sales_volume as varchar) AS mortgage_id, merged_data.cash_average_price, merged_data.cash_sales_volume,
merged_data.mortgage_average_price, merged_data.mortgage_sales_volume FROM merged_data
GROUP BY merged_data.area_code|| cast(merged_data.cash_sales_volume as varchar), merged_data.cash_average_price, merged_data.cash_sales_volume,
merged_data.mortgage_average_price, merged_data.mortgage_sales_volume")
writeTablesToHive(DimMortage, mortgage_id)

################################# Create Factables  #############################

# Create table FactSales_1
dbGetQuery(con,"CREATE TABLE IF NOT EXISTS FactSales_1(area_code string, time_id string, SumOfno_of_sales int, property_Id string, depression_id int)")
FactSales_1 <- sqldf("SELECT DimRegion.area_code, DimTime.time_id, Sum(merged_data.no_of_sales) AS SumOfno_of_sales, DimProperty.property_id, DimDepression.depression_id
FROM DimDepression INNER JOIN (DimProperty INNER JOIN (DimTime INNER JOIN (DimRegion INNER JOIN merged_data ON DimRegion.area_code = merged_data.area_code) ON DimTime.time_id = merged_data.date)
ON DimProperty.property_type = merged_data.property_type) ON DimDepression.imd_score = merged_data.imd_score
GROUP BY DimRegion.area_code, DimTime.time_id, DimProperty.property_Id, DimDepression.depression_ID")
writeTablesToHive(FactSales_1, area_code)


# Create table FactAvgPrice
dbGetQuery(con,"CREATE TABLE IF NOT EXISTS Fact_AvgPrice(area_code string, property_Id string,time_id string,
           depression_id int, SumOfaverage_price double)")
#dbGetQuery(con, "drop table Fact_AvgPrice")
Fact_AvgPrice <- sqldf("SELECT DimRegion.area_code, DimProperty.property_Id, DimTime.time_id, DimDepression.depression_id, Sum(merged_data.average_price) AS SumOfaverage_price
FROM (((merged_data INNER JOIN DimRegion ON merged_data.area_code = DimRegion.area_code) INNER JOIN DimDepression ON merged_data.imd_score = DimDepression.imd_score) INNER JOIN DimTime 
ON merged_data.date = DimTime.time_id) INNER JOIN DimProperty ON merged_data.property_type = DimProperty.property_type
GROUP BY DimRegion.area_code, DimProperty.property_Id, DimTime.time_id, DimDepression.depression_id")
writeTablesToHive(Fact_AvgPrice, area_code)


# Create table FactSales_2
dbGetQuery(con,"CREATE TABLE IF NOT EXISTS FactSales_2(area_code string, depression_id string,time_id string,
           SumOfcash_sales_volume double,SumOfmortgage_sales_volume double, mortgage_id string)")
FactSales_2 <- sqldf("SELECT DimRegion.area_code AS area_code, DimDepression.depression_id, DimTime.time_id,
Sum(merged_data.cash_sales_volume) AS SumOfcash_sales_volume, Sum(merged_data.mortgage_sales_volume)
AS SumOfmortgage_sales_volume, DimMortage.mortgage_id 
FROM DimMortage INNER JOIN (DimDepression INNER JOIN ((merged_data INNER JOIN DimTime ON
merged_data.date = DimTime.time_id) INNER JOIN DimRegion ON merged_data.area_code = DimRegion.area_code) ON
DimDepression.imd_score = merged_data.imd_score) ON DimMortage.cash_average_price = merged_data.cash_average_price
GROUP BY DimRegion.area_code, DimDepression.depression_id, DimTime.time_id, DimMortage.mortgage_id")
writeTablesToHive(FactSales_2, area_code)


## Create table FactCrime
dbGetQuery(con,"CREATE TABLE IF NOT EXISTS FactSales_2(area_code string, depression_id string,time_id string,
           SumOfno_of_sales int,crime_score double, SumOfaverage_price double)")
Fact_Crime <- sqldf("SELECT DimRegion.area_code as area_code , DimDepression.depression_id, DimTime.time_id,
Sum(merged_data.no_of_sales) AS SumOfno_of_sales, DimDepression.crime_score, Sum(merged_data.average_price) 
AS SumOfaverage_price FROM DimDepression INNER JOIN ((merged_data INNER JOIN DimTime ON
merged_data.date = DimTime.time_id) INNER JOIN DimRegion ON merged_data.area_code = DimRegion.area_code) ON
DimDepression.imd_score = merged_data.imd_score GROUP BY DimRegion.area_code, DimDepression.depression_id,
                    DimTime.time_id, DimDepression.crime_score")
writeTablesToHive(Fact_Crime, area_code)