

#------------------------------------
########################### EXAM 2023
#------------------------------------

#_____________________________________ LIBRARIES

library(FNN)
library(ggplot2)
library(readxl)
library(MASS)
library(mvtnorm)
library(mclust)
library(posterior)
library(cluster)
library(dplyr)



getwd()
setwd("C:\\Users\\Lance Fick\\Desktop\\MIT Big Data\\MIT805-Big Data\\Assignment One")
getwd()

data <- read.csv('used_cars_data.csv', header = TRUE)



# Subset the data to include only specific columns
subset_data <- data[c("vin"
                      ,"price"
                      ,"body_type"
                      ,"engine_displacement"
                      ,"horsepower"
                      ,"make_name"
                      ,"mileage" 
                      ,"transmission"  
                      ,"wheel_system")]

#------------------------ distinct variable understanding

#---------------------------------------------------- body type
distinct_body_type <- unique(subset_data$body_type) 
distinct_body_type

#---------------------------------------------------- engine_displacement
distinct_engine_displacement<- unique(subset_data$engine_displacement) 
distinct_engine_displacement

#---------------------------------------------------- Horsepower
distinct_horsepower<- unique(subset_data$horsepower) 
distinct_horsepower

#----------------------------------------------------Make name
distinct_make_name<- unique(subset_data$make_name) 
distinct_make_name

#----------------------------------------------------mileage
distinct_mileage<- unique(subset_data$mileage) 
distinct_mileage

#----------------------------------------------------transmission
distinct_transmission<- unique(subset_data$transmission) 
distinct_transmission

#----------------------------------------------------wheel_system
distinct_wheel_system<- unique(subset_data$wheel_system) 
distinct_wheel_system



#datatype:

sapply(subset_data,class)


#descriptive Statistics 
summary(subset_data)

#summary views of distribution:

result <- subset_data %>%
  group_by(body_type) %>%
  summarise(count = n_distinct(vin))
print(result)

unique_value <-length(distinct_body_type)
print(unique_value)


result <- subset_data %>%
  group_by(transmission) %>%
  summarise(count = n_distinct(vin))

print(result)

unique_value <-length(distinct_transmission)
print(unique_value)


result <- subset_data %>%
  group_by(make_name) %>%
  summarise(count = n_distinct(vin)) %>%
  arrange(desc(count))
  
print(result, n=10) #show topten 

unique_value <-length(distinct_make_name)
print(unique_value)



## Price distribution of the column 

result <- subset_data %>%
  group_by(make_name) %>%
  summarise (total_price = mean(price)) %>%
  arrange(desc(total_price))

print(result) #show topten 


subset_data2 <- subset_data
breaks <- c(0,2000,4000,8000,16000
            ,32000,64000,100000,200000
            ,300000)

subset_data2$mileband <-cut(subset_data$mileage,breaks = breaks, labels =c( "0 to 2k","2k to 4k","4k to 8k","8k to 16k","16k to 32k", 
                                                                           "32k to 64k","64k to 100k","100k to 200k","200k to 300k" ))

distinct_mileband<- unique(subset_data2$mileband) 
distinct_mileband

result <- subset_data2 %>%
  group_by(mileband) %>%
  summarise (total_price = mean(price)) %>%
  arrange(desc(total_price))

print(result) #show topten 