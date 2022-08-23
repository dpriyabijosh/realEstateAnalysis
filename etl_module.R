
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
prepareEnvr(packages)

#SetUpConfiguration
log_open("admp.log")

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
mortgage <- fread(path_mortgage,select = c(1,2,3,8,13),data.table = FALSE)
xlsFile = "social_deprevation.xlsx"
download.file(url=path_deprivation, destfile=xlsFile, mode="wb")
social_deprivation <- read_excel(xlsFile,2)

######################### Data Preprocessing ############################################### 
###############Property Dataset########################################################

log_print("######### Property Dataset ###############")
skim(property)

# Data transformation from date to yearmonth format
property$date <- format(as.Date(property$date,format = "%Y-%m-%d"),"%Y%m")

# Subset data from 2016 to 2019
property <- subset(property, date %in% 201601:201912)
skim(property)
head(property)

log_print("Property dataframe initial dimensions")
log_print(dim(property))

property <- property %>%
  dplyr::rename(
    region_name = 'district'
  )

#make all values in the region name to lower case before data merging
property$region_name <-tolower(property$region_name)

property$region_name <- recode(property$region_name,
                               'east dorset' = 'dorset',
                               'forest heath' ='suffolk',
                               'north dorset' = 'dorset',
                               'north northamptonshire' = 'northamptonshire',
                               'poole' = 'dorset',
                               'purbeck'='dorset',
                               'shepway' = 'kent',
                               'st edmundsbury' ='suffolk',
                               'suffolk coastal' = 'suffolk',
                               'taunton deane' = 'somerset',
                               'waveney' = 'suffolk',
                               'west dorset' = 'dorset',
                               'weymouth and portland'= 'dorset',
                               'west somerset'= 'somerset',
                               'west northamptonshire' = 'northamptonshire',
                               'bournemouth, christchurch and poole' = 'bournemouth',
                               
                               
)

# Clean the column names
property<-clean_names(property)

# Calculating the average Price and number of sales
tempProperty <- property
tempProperty$no_of_sales <- ""
tempProperty <- aggregate(no_of_sales~ date + property_type  + build_type  + tenure + region_name + county,
                          data = tempProperty , FUN = length)

property <- aggregate(cbind(price) ~ date + property_type  + build_type  + tenure + region_name + county,
                      data = property, FUN = mean)
property$no_of_sales <- tempProperty$no_of_sales 
log_print("Property dataframe initial dimensions")
log_print(dim(property))

rm(tempProperty)

# Rename price to average_price
property <- property %>%
  dplyr::rename(
    average_price = "price",
  )

#make all values in the region name to lower case before data merging
property$region_name <-tolower(property$region_name)
head(property)

####################### Data Validations  ########################################
# Function to check duplicates
qualityChecks_duplicates <- function(df){
  cat('\nThe number of duplicate entries in the data are', sum(duplicated(df)))
}
qualityChecks_duplicates(property)

# Function to drop duplicate
manageDuplicate<- function(df){
  cat('\nThe size of data frame before removing duplicates', dim(df))
  # Validate the duplicates in the data
  cat('\nThe number of duplicate entries in the data are', sum(duplicated(df)))
  cat("\n")
  # Remove duplicates
  df <- df %>% distinct()
  log_print("After duplicate removal :")
  log_print(dim(df))
  cat('\nAfter removing the duplicates the shape of the dataframe is', dim(df))
  return(df)
}
property <- manageDuplicate(property)

####################### Data Cleaning  ########################################
cat('The shape of the dataframe is', dim(property))

# Function to check the  missing value handling
qualityChecks_missingValue <- function(df){
  cat("\nThe total number of null values in the dataset ", sum(is.na(df)))
  if(sum(is.na(df))>1){
    print("View Rows with Null values :")
    print(df[rowSums(is.na(df)) > 0, ])
  }
}
qualityChecks_missingValue(property)

# Function to perform missing value handling
manageNullValues<- function(df){
  log_print("Initial size of dataframe before null value handling")
  log_print(dim(df))
  # view missing values from the dataset
  missingValues <- sum(is.na(df))
  log_print("Number of missing values:")
  log_print(missingValues)
  df1 <- df[rowSums(is.na(df)) > 0, ]
  head(df1, 5)
  library(imputeTS)
  df <- na_mean(df)
  log_print("After mean value imputation the original data size")
  log_print(dim(df))
  cat("\nThe total number of null values after mean imputation ", sum(is.na(df)))
  cat("\n")
  # cat('\nView missing values\n')
  # missingValues <- df[rowSums(is.na(df)) > 0, ]
  #log_print(dim(missingValues))
  log_print("Checking the number of categrical missing values:")
  missingValues <- sum(is.na(df)) 
  log_print(missingValues)
  head(df[rowSums(is.na(df)) > 0, ])
  df <- df %>% drop_na()
  cat("\nThe total number of null values after categorical values null values", sum(is.na(df)))
  cat("\n")
  log_print("After removing categorical value the original data size")
  log_print(dim(df))
  return(df)
}

property <- manageNullValues(property)

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
  boxplot(df)
  for (col in cols) {
    df <- df[!outliers(df[[col]]),]
  }
  df
  boxplot(df)
}

dim(property)
head(property)
skim(property)

log_print("######################### Mortgage ##########################")

##################################################################
# Mortgage Dataset
mortgage$Date <- format(as.Date(mortgage$Date,format = "%Y-%m-%d"),"%Y%m")
mortgage <- subset(mortgage, Date %in% 201601:201912)
log_print("The initial shape of mortage dataframe")
log_print(dim(mortgage))

################ Data Cleaning #######################

#Removing Wales and Scotland and Northern Ireland from the dataset
mortgage <- subset(mortgage,  (grepl('E', Area_Code )==TRUE))
log_print("The initial shape of mortage dataframe")
log_print(dim(mortgage))
skim(mortgage)

cat('The shape of the dataframe is', dim(mortgage))

mortgage<-clean_names(mortgage)
head(mortgage)

# Make all the values of region names are lower
mortgage$region_name  = tolower(mortgage$region_name)
mortgage$area_code = tolower(mortgage$area_code)

mortgage$region_name <- recode(mortgage$region_name,
                               'bournemouth christchurch and poole' = 'bournemouth',
                               'telford and wrekin' ='wrekin'
)
dim(mortgage)

# Duplicate value handling
qualityChecks_duplicates(mortgage)
mortgage <- manageDuplicate(mortgage)

#Missing value handling
qualityChecks_missingValue(mortgage)
mortgage <- manageNullValues(mortgage)
skim(mortgage)

##################################################################################
#####Social Deprivation###################
log_print("#########Social Deprivation#########")

social_deprivation <- read_excel(xlsFile,2)

log_print("Initial size of social depression dataset")
log_print(dim(social_deprivation))

skim(social_deprivation)

# Remove unwanted columns
social_deprivation <-social_deprivation[ , c(3,5,6,7,10)]

log_print("Subsetting interested columns social depression dataset")
log_print(dim(social_deprivation))
head(social_deprivation)

# Rename the column names
social_deprivation <- social_deprivation %>%
  dplyr::rename(
    area_code = "Local Authority District code (2019)",
    imd_score = "Index of Multiple Deprivation (IMD) Score",
    income_score = "Income Score (rate)",
    employment_score = "Employment Score (rate)",
    crime_score = "Crime Score"
  )
dim(social_deprivation)

# Aggregate data to find average deprivation score on each category based on district
social_deprivation <- aggregate(social_deprivation,
                                by = list(social_deprivation$area_code),
                                FUN = mean)

#social_deprivation[ , c("region_name")] <- list(NULL)
social_deprivation[ , c("area_code")] <- list(NULL)
head(social_deprivation)

social_deprivation <- social_deprivation %>%
  dplyr::rename(
    area_code ='Group.1'
  )

log_print("After taking average deprevation score")
log_print(dim(social_deprivation))

head(social_deprivation)

# Making standardized Header names and making all region_name to lower
social_deprivation<-clean_names(social_deprivation)

social_deprivation$area_code  = tolower(social_deprivation$area_code)
head(social_deprivation)

# NUll Value handling
qualityChecks_missingValue(social_deprivation)
social_deprivation <- manageNullValues(social_deprivation)

# Duplicate removal
social_deprivation <- manageDuplicate(social_deprivation)

skim(social_deprivation)

######################################## Data Merge ############################################## 
log_print('################### Merged Data ##########################')

df_list <- list(property,mortgage)
# Merge the property and mortage data
merged_data <- Reduce(function(x, y) merge(x, y, by=c("region_name","date"), all.x=TRUE), df_list, accumulate=FALSE)
head(merged_data, n = 5)

log_print('Size of the initial merged Data')
log_print(dim(merged_data))


df_list <- list(merged_data,social_deprivation)
# Merge the social deprivation with the initial merge data
merged_data <- Reduce(function(x, y) merge(x, y, by=c("area_code"), all.x=TRUE), df_list, accumulate=FALSE)
dim(merged_data)
head(merged_data, 5)

df <- merged_data[rowSums(is.na(merged_data)) > 0, ]
tail(df,5)

log_print('Size of the final merged Data')
log_print(dim(merged_data))

# Handling missing values
qualityChecks_missingValue(merged_data)
mergeta <- manageNullValues(merged_data)

# Duplicate removal
mergeta <- manageDuplicate(merged_data)

log_print('Size of Cleaned merged Data')
log_print(dim(merged_data))

dim(merged_data)
head(merged_data)


#### Clearing unwanted dataframes created #################
rm(property, mortgage, social_deprivation, path_deprivation,path_mortgage,path_property, xlsFile)
skim(merged_data)
##################################### Data Loading ########################################################
prepareEnvr(packages)
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
dbGetQuery(con,"CREATE TABLE IF NOT EXISTS DimRegion(region_id string, region_name string, county string, PRIMARY KEY(area_code) disable novalidate)")

# Create a dataframe for DimRegion
DimRegion <- sqldf("SELECT merged_data.area_code as region_id, merged_data.region_name, merged_data.county FROM merged_data
      GROUP BY merged_data.area_code, merged_data.region_name, merged_data.county")

# Write the dataframe to Hive datatable
writeTablesToHive(DimRegion,region_id)



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

# Create DimDepression
dbGetQuery(con,"CREATE TABLE  IF NOT EXISTS DimDepression(depression_id int,imd_score string, income_score string,
    employment_score string, crime_score string, PRIMARY KEY(depression_id) disable novalidate)")

DimDepression <- sqldf("SELECT row_number() over () as depression_id, merged_data.imd_score, merged_data.income_score,
merged_data.employment_score, merged_data.crime_score FROM merged_data GROUP BY
merged_data.imd_score, merged_data.income_score, merged_data.employment_score,
                       merged_data.crime_score")

writeTablesToHive(DimDepression, depression_id)


#Create DimMortage
dbGetQuery(con,"CREATE TABLE  IF NOT EXISTS DimMortgage(mortgage_id int, cash_average_price string, cash_sales_volume string,
mortgage_average_price string, mortgage_sales_volume string, primary key(mortgage_id) disable novalidate)")

DimMortgage <- sqldf("SELECT (cast(SUBSTRING(merged_data.area_code,3,4) as int)|| (row_number() over ())) AS mortgage_id, merged_data.cash_average_price, merged_data.cash_sales_volume,
merged_data.mortgage_average_price, merged_data.mortgage_sales_volume FROM merged_data
GROUP BY merged_data.cash_average_price, merged_data.cash_sales_volume,
merged_data.mortgage_average_price, merged_data.mortgage_sales_volume")

writeTablesToHive(DimMortgage, mortgage_id)

################################# Create Factables  #############################

# Create table Fact_Sales
dbGetQuery(con,"CREATE TABLE IF NOT EXISTS Fact_Sales(region_id string, time_id string, SumOfno_of_sales int, property_Id string, depression_id int)")

Fact_Sales <- sqldf("SELECT DimRegion.region_id, DimTime.time_id, Sum(merged_data.no_of_sales) AS SumOfno_of_sales, DimProperty.property_id, DimDepression.depression_id
FROM DimDepression INNER JOIN (DimProperty INNER JOIN (DimTime INNER JOIN (DimRegion INNER JOIN merged_data ON DimRegion.region_id = merged_data.area_code) ON DimTime.time_id = merged_data.date)
ON DimProperty.property_type = merged_data.property_type) ON DimDepression.imd_score = merged_data.imd_score
GROUP BY DimRegion.region_id, DimTime.time_id, DimProperty.property_Id, DimDepression.depression_ID")

writeTablesToHive(Fact_Sales, region_id)


# Create table FactAvgPrice
dbGetQuery(con,"CREATE TABLE IF NOT EXISTS Fact_AvgPrice(area_code string, property_Id string,time_id string,
           depression_id int, SumOfaverage_price double)")

Fact_AvgPrice <- sqldf("SELECT DimRegion.region_id, DimProperty.property_Id, DimTime.time_id, DimDepression.depression_id, Sum(merged_data.average_price) AS SumOfaverage_price
FROM (((merged_data INNER JOIN DimRegion ON merged_data.area_code = DimRegion.region_id) INNER JOIN DimDepression ON merged_data.imd_score = DimDepression.imd_score) INNER JOIN DimTime 
ON merged_data.date = DimTime.time_id) INNER JOIN DimProperty ON merged_data.property_type = DimProperty.property_type
GROUP BY DimRegion.region_id, DimProperty.property_Id, DimTime.time_id, DimDepression.depression_id")

writeTablesToHive(Fact_AvgPrice, area_code)


# Create table Fact_mortgage
dbGetQuery(con,"CREATE TABLE IF NOT EXISTS Fact_mortgage(area_code string, depression_id string,time_id string,
           SumOfcash_sales_volume double,SumOfmortgage_sales_volume double, mortgage_id string)")

Fact_mortgage <- sqldf("SELECT DimRegion.region_id AS area_code, DimDepression.depression_id, DimTime.time_id,
Sum(merged_data.cash_sales_volume) AS SumOfcash_sales_volume, Sum(merged_data.mortgage_sales_volume)
AS SumOfmortgage_sales_volume, DimMortage.mortgage_id 
FROM DimMortage INNER JOIN (DimDepression INNER JOIN ((merged_data INNER JOIN DimTime ON
merged_data.date = DimTime.time_id) INNER JOIN DimRegion ON merged_data.area_code = DimRegion.region_id) ON
DimDepression.imd_score = merged_data.imd_score) ON DimMortage.cash_average_price = merged_data.cash_average_price
GROUP BY DimRegion.region_id, DimDepression.depression_id, DimTime.time_id, DimMortage.mortgage_id")

writeTablesToHive(Fact_mortgage, area_code)


## Create table Fact_Crime
dbGetQuery(con,"CREATE TABLE IF NOT EXISTS Fact_Crime(area_code string, depression_id string,time_id string,
           SumOfno_of_sales int,crime_score double, SumOfaverage_price double)")
Fact_Crime <- sqldf("SELECT DimRegion.region_id as area_code , DimDepression.depression_id, DimTime.time_id,
Sum(merged_data.no_of_sales) AS SumOfno_of_sales, DimDepression.crime_score, Sum(merged_data.average_price) 
AS SumOfaverage_price FROM DimDepression INNER JOIN ((merged_data INNER JOIN DimTime ON
merged_data.date = DimTime.time_id) INNER JOIN DimRegion ON merged_data.area_code = DimRegion.region_id) ON
DimDepression.imd_score = merged_data.imd_score GROUP BY DimRegion.region_id, DimDepression.depression_id,
                    DimTime.time_id, DimDepression.crime_score")
writeTablesToHive(Fact_Crime, crime_id)

log_close()