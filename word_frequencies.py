#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
import pyspark.sql.functions as F


# In[2]:


spark = (SparkSession.builder.appName("Compute BoW Vectors").getOrCreate())


# In[ ]:


results = (
    spark.read.text("all-the-news-2-1.csv")
    .select(F.split(F.col("_c8"), " ").alias("article"))
    .select(F.explode(F.col("article")).alias("tokens"))
    .select(F.lower(F.col("tokens")).alias("normalized tokens"))
    .select(F.regexp_extract(F.col("normalized tokens"), "[a-z']*", 0).alias("cleaned tokens"))
    .where(F.col("cleaned tokens") != "")
    .groupby("cleaned tokens")
    .count()
)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




