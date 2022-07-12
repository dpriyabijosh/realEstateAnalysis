# prepareEnvr function: install and load multiple R packages.
# check to see if packages are installed. Install them if they are not, then load them into the R session.
prepareEnvr <- function(pkg){
  new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
  if (length(new.pkg)) 
    install.packages(new.pkg, dependencies = TRUE)
  sapply(pkg, require, character.only = TRUE)
}

# Prepare Environment
packages <- c("dplyr","tidyverse", "stringr","plyr","reshape","skimr","repr","janitor")
prepareEnvr(packages)

# Create variables for paths of the datasets
path_crime <- "https://data.london.gov.uk/download/recorded_crime_summary/2bbd58c7-6be6-40ac-99ed-38c0ee411c8e/MPS%20Borough%20Level%20Crime%20%28Historical%29.csv"
path_housePrice <-"http://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/Average-prices-2022-03.csv?utm_medium=GOV.UK&utm_source=datadownload&utm_campaign=average_price&utm_term=9.30_18_05_22"
path_salesVolume <- "http://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/Sales-2022-03.csv?utm_medium=GOV.UK&utm_source=datadownload&utm_campaign=sales&utm_term=9.30_18_05_22"

######################### Data Loading #####################################################

# Load crime data from the csv file
crime <- read.csv(path_crime,stringsAsFactors = TRUE, fileEncoding = "UTF-8")
# Load House price dataset and Verify quick overview of data 
housePrice <- read.csv( path_housePrice, stringsAsFactors = TRUE, fileEncoding = "UTF-8")
# Load sales volume dataset and verify quick overview of data
salesData <- read.csv(file = path_salesVolume, stringsAsFactors = TRUE, fileEncoding = "UTF-8")

# Check the shape and quick summary of row data sets

# Check crime data
head(crime)
skim(crime)
cat('The shape of the data set is ', nrow(crime),'*', ncol(crime),'.')

# Check average price data
head(housePrice)
skim(housePrice)
cat('The shape of the data set is ', nrow(housePrice),'*', ncol(housePrice),'.')

# Check the sales volume data
head(salesData)
skim(salesData)
cat('The shape of the data set is ', nrow(salesData),'*', ncol(salesData),'.')


######################### Data Preprocessing ############################################### 
 ##### Crime Data #######
# Removing unwanted from the YearMonth Column from Crime
names(crime) = gsub(pattern = "*X", replacement="", x=names(crime)) 
head(crime) 
cat('The shape of the data set is ', nrow(crime),'*', ncol(crime),'.') # verify the shape of dataframe 

# Check the Region_Name to identify the boroughs list for necessary transformation
print(unique(crime$LookUp_BoroughName))
# Replace "London Heathrow and London City Airports" with "City of London" and "Westminster" with "City of Westminster"
#       to avoid data loss while merging with average price and sales volume dataset
crime$LookUp_BoroughName <- recode(crime$LookUp_BoroughName,
                  'London Heathrow and London City Airports' = 'City of London',
                  'Westminster'  = 'City of Westminster',
                  )

# Validate the LookUp_BoroughName column to verify the replaced borough names 
print(unique(crime$LookUp_BoroughName))
cat('The shape of the data set is ', nrow(crime),'*', ncol(crime),'.')

# Reshape the data frame by adding 2 variables such as variable(the yearmonth) and value(nvalue of each yearmonth and borough).
crime <- melt(crime) 
head(crime)
# Renamed the columns headers as CrimeType, Region_Name, Date and NoOfCrime for MajorText, LookUp_BoroughName, variable and value.
crime <- crime %>% 
  dplyr::rename(
    CrimeType = MinorText,
    CrimeCategory = MajorText,
    Region_Name = LookUp_BoroughName,
    Date = variable,
    NoOfCrime = value
  )
head(crime)

# Verify quick overview of data and shape
skim(crime)
cat('The shape of the data set is ', nrow(crime),'*', ncol(crime),'.') 

##### House Price Data ############
# Replace the data column with month and date
housePrice$Date <- format(as.Date(housePrice$Date,format = "%Y-%m-%d"),"%Y%m")
# Check first few rows to check the date with new format
head(housePrice) 
# Check the shape
cat('The shape of the average price is',dim(housePrice)) 

##### Sales Data ##################
# Replace the data column with month and date
salesData$Date <- format(as.Date(salesData$Date,format = "%Y-%m-%d"),"%Y%m")
head(salesData)
cat('The shape of the sales volume is', dim(salesData))

######################### Data Merge ############################################### 
# Merge the 3 datasets by Date and borough
df_list <- list(crime, housePrice, salesData) 
merged_data <- Reduce(function(x, y) merge(x, y, by=c("Region_Name","Date"), all=TRUE), df_list, accumulate=FALSE)
head(merged_data, n = 10)
glimpse(merged_data)
skim(merged_data)
cat('The shape of the  merged data set before processing ', nrow(merged_data),'*', ncol(merged_data),'.') 

######################### Data Transformation ############################################### 

# Added a new column revenue by multiplying Average_Price and Sales_column
# Values are rounded  as part of normalization
transformed_data <- merged_data %>% 
  mutate(revenue = Average_Price * Sales_Volume) %>% 
  mutate_if(is.numeric, round)
head(transformed_data)
cat('The shape of the transformed data ', nrow(transformed_data),'*', ncol(transformed_data),'.')

######################### Data cleaning and validation  ####################################

# Remove data with selected time period
data_cleaned <- subset(transformed_data, Date %in% 201601:202012)
# data_cleaned <- data_cleaned[with(data_cleaned, order(Date, Region_Name)), ]
data_cleaned <- arrange(data_cleaned,Date,Region_Name)
head(data_cleaned)
# Check shape
cat('The shape of the data within the time period ', nrow(data_cleaned),'*', ncol(data_cleaned),'.') 
cat('Removed row',(nrow(transformed_data)-nrow(data_cleaned)),'\nRemoved columns',(ncol(transformed_data)-ncol(data_cleaned)))
head(data_cleaned)

# Remove unwanted columns
data_cleaned[ , c('Area_Code.y','Monthly_Change','Annual_Change','Average_Price_SA')] <- list(NULL)
data_cleaned$Area_Code <- data_cleaned$Area_Code.x
head(data_cleaned)
cat('The shape of the data after dropping unwanted columns', nrow(data_cleaned),'*', ncol(data_cleaned),'.') # checking the shape

# Group rows with same type of crime, date and borough by taking total of crimes.
data_cleaned <- aggregate( cbind( NoOfCrime ) ~ Region_Name + Area_Code + Date + CrimeCategory + CrimeType + Average_Price + Sales_Volume + revenue,
                    data = data_cleaned , FUN = sum)
head(data_cleaned)
cat('The shape of the data after aggregation ', nrow(data_cleaned),'*', ncol(data_cleaned),'.') # checking the shape
glimpse(data_cleaned)
skim(data_cleaned)

# Clean the column names
data_cleaned<-clean_names(data_cleaned)
head((data_cleaned))

# Remove unwanted observations where number of crimes are 0.
data_cleaned<-data_cleaned[data_cleaned$no_of_crime != 0, ]
head(data_cleaned)
cat('The shape of the dataframe is', dim(data_cleaned))

# Validate missing values in the dataset
any(is.na(data_cleaned))
cat("The total number of null values in the dataset ", sum(is.na(data_cleaned)))


# Remove missing values from the dataset
data_cleaned <- data_cleaned %>% drop_na()
cat('The shape of the dataframe is', dim(data_cleaned))
glimpse(data_cleaned)
skim(data_cleaned)

# Validate rows with missing values
rownames(data_cleaned)[apply(data_cleaned, 2, anyNA)]
cat('The shape of the dataframe is', dim(data_cleaned))

# Remove empty rows and columns of data
data_cleaned <- remove_empty(data_cleaned, which = c("rows","cols"), quiet = FALSE)
cat('The shape of the dataframe is', dim(data_cleaned))

# Validate the duplicates in the data
cat('The number of duplicate entries in the data are', sum(duplicated(data_cleaned)))

# Remove duplicates
data_cleaned <- data_cleaned %>% distinct()
head(data_cleaned)
cat('The shape of the dataframe is', dim(data_cleaned))

#Sort the data with boroughname and date 
data_cleaned <- data_cleaned[with(data_cleaned, order(date,region_name)),]
head(data_cleaned)
cat('The shape of the dataframe is', dim(data_cleaned))


# Write the dataset to single csv file to check the merged dataset
write.csv(data_cleaned, file = "data.csv", row.names = FALSE)
