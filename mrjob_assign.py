#!/usr/bin/env python
# coding: utf-8

# In[3]:


get_ipython().system('pip install pyspark')
get_ipython().system('pip install pyspark[pandas_on_spark] plotly')
get_ipython().system('pip install -U pandas')
get_ipython().system('pip install dataprep')
get_ipython().system('pip install sweetviz')


# In[4]:


import gc
import pyspark
import pandas as pd
import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col,sum
from IPython.display import HTML, display
from sklearn.cluster import KMeans
from pyspark.sql import functions as F
from dataprep.eda import create_report
from pyspark.mllib.linalg import Vectors
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark import SparkContext
from pyspark.sql import SparkSession
import sweetviz as sv


get_ipython().run_line_magic('matplotlib', 'inline')


# In[ ]:





# In[76]:





# In[7]:


pip show pyspark


# In[8]:


import pandas as pd
from functools import reduce


# In[9]:


#spark=SparkSession.builder.appName('Market Segmentation').getOrCreate()


# In[10]:


#df_market=spark.read.option('header','true').csv('C:/Users/Lance Fick/Desktop/MIT Big Data/MIT805-Big Data/Assignment one//used_cars_data.csv')


# In[11]:


import os


# In[12]:


os.chdir("C:/Users/Lance Fick/Desktop/MIT Big Data/MIT805-Big Data/Assignment one")


# In[13]:


files_in_directory = os.listdir()


# In[14]:


print("Files in the current working directory:", files_in_directory)


# In[15]:


#spark=SparkSession.builder.appName('Market Segmentation').getOrCreate()


# In[16]:


#spark


# In[17]:


#df_market=spark.read.option('header','true').csv('/content/drive/My Drive/MIT 805/used_cars_data.csv')


# In[18]:


# Read the CSV data from the current working directory
data = pd.read_csv("used_cars_data.csv")


# In[19]:


top_10_rows = data.head(10)


# In[20]:


top_10_rows


# In[ ]:





# In[21]:


# Get the column names
column_names = data.columns

# Display the column names
column_names 


# In[22]:


selected_columns = data[[ "price","body_type", "engine_cylinders", 
                         "engine_displacement", "exterior_color", 
                         "horsepower", "interior_color", 
                         "make_name", "mileage", 
                         "owner_count", "transmission", 
                         "wheel_system"]]


# In[23]:


top_10_rows = selected_columns.head(10)


# In[24]:


top_10_rows


# In[25]:


pip install mrjob  


# In[2]:


pip show mrjob


# In[26]:


from mrjob.job import MRJob # import the MRjob library. mapreducer in python
from mrjob.step import MRStep 

class MRUsedCar(MRJob):       #job is call MRUsedCar(has all capabilities defined my MRjob) the defines the mapper andthe reducer 
    def steps(self):           #"steps" it tell framework what functions are used for mappers and reducers.   
        return [
            MRStep(mapper=self.mapper_get_body_type,
                   reducer=self.reducer_count_body_type)
        ]

    #defining a mapper. it takes three aruguments: self, key, record
    def mapper_get_body_type(self, _,line):
        (price, body_type,engine_displacement,horsepower,make_name,mileage,transmission,wheel_system) = line.split('\t')
        yield body_type,price, 1  
         
    def reducer_count_body_type(self, key, values):    #defining a reducer.
        yield body_type, sum(values)
        
if __name__ == '__MRUsedCar__':
   MRUsedCar.run()
 
    
from mrjob.job import MRJob # import the MRjob library. mapreducer in python
from mrjob.step import MRStep 



class MRUsedCar2(MRJob):       #job is call MRUsedCar(has all capabilities defined my MRjob) 
                                #the defines the mapper andthe reducer 
    def steps(self):           #"steps" it tell framework what functions are used for mappers and reducers.   
        return [
            MRStep(mapper=self.mapper_get_horsepower,
                   reducer=self.reducer_count_horsepower)
        ]

    #defining a mapper. it takes three aruguments: self, key, record
    def mapper_get_horsepower(self, _,line): #This is where you define the logic for the map phase of your MapReduce job. 
                                            #it reads input variables & extracts key-values
        (price, body_type,engine_displacement,horsepower,make_name,mileage,transmission,wheel_system) = line.split('\t')
        yield horsepower,price, 1  
         
    def reducer_count_horsepower(self, key, values):    #defining a reducer.
        yield horsepower, sum(values)
        
if __name__ == '__MRUsedCar2__':
   MRUsedCar.run()
     


# In[37]:


selected_columns.count()


# In[40]:


selected_columns.dtypes


# In[41]:


selected_columns_adj = selected_columns.dropDuplicates()


# In[44]:


selected_columns.describe().show() 


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




