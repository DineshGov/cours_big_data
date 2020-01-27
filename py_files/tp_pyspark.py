#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import *


# In[2]:


sc = SparkContext(master="local", appName="test")


# In[3]:


sc


# In[4]:


import urllib
data = urllib.request.urlretrieve('http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data_10_percent.gz', 'data_10_percent.gz')
data_file = './data_10_percent.gz'

lines = sc.textFile(data_file)


# In[5]:


from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


# In[6]:


sqlContext


# In[7]:


data = lines.map(lambda x: x.split(','))
data.take(5)


# In[8]:


type(data)


# In[9]:


from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType


# In[10]:


schema = StructType([
    StructField("duration", IntegerType(), True),
    StructField("protocol_type", StringType(), True),
    StructField("service", StringType(), True),
    StructField("flag", StringType(), True),
    StructField("src_bytes", IntegerType(), True),
    StructField("dst_bytes", IntegerType(), True),
    StructField("interactions", StringType(), True),
    ])
schema


# In[11]:


type(schema)


# In[12]:


newData = data.map(lambda key:(int(key[0]),key[1],key[2],key[3],int(key[4]),int(key[5]),key[-1]))
newData.take(3)


# In[16]:


df = sqlContext.createDataFrame(newData, schema)
df


# In[17]:


df.show(10)


# In[19]:


df.createOrReplaceTempView("interactions")
sqlContext.sql("SELECT interactions FROM sql_view").show()

