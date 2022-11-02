#!/usr/bin/env python
# coding: utf-8

# In[2]:


get_ipython().system('pip3 install pyspark')


# In[12]:


from pyspark.sql import SparkSession


# In[13]:


# spark = SparkSession.Builder.appName('Dataframe').getOrCreate()
spark=SparkSession.builder.appName('Dataframe').getOrCreate()


# In[14]:


spark


# In[16]:


df_pyspark=spark.read.option('header','true').csv('Department_Dataset 2.csv',inferSchema=True)


# In[17]:


df_pyspark.printSchema()


# In[18]:


df_pyspark.show()


# In[19]:


df_pyspark.head(5)


# In[20]:


df_pyspark.select(['Dept_name','location'])


# In[21]:


df_pyspark.select(['Dept_name','location']).show()


# In[22]:


df_pyspark.dtypes


# In[ ]:


df_pyspark.withColumn('travel',)


# In[23]:


from pyspark.sql.functions import when

matches = df_pyspark["travel_required"].isin("yes")
new_df = df_pyspark.withColumn("travel_required", when(matches, 1).otherwise(0))


# In[24]:


new_df.show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




