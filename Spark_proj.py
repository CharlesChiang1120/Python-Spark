#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkContext
from pyspark.sql import SparkSession
import os
import re
import datetime
import pandas as pd
import json
from pyspark.sql import functions as F
from pyspark.sql.functions import *


# In[2]:


# Spark Core
sc = SparkContext('local')
spark = SparkSession(sc)


# In[3]:


spark = SparkSession.builder.appName('Read All CSV Files in Directory').getOrCreate()

# 讀取所有csv結尾的檔案
file2 = spark.read.csv('./*.csv', sep=',', inferSchema=True, header=True)
 
# 使用Pandas
df1 = file2.toPandas()

# 檢查資料頭尾
display(df1.head())
display(df1.tail())


# In[4]:


# 驗證台中市的資料
df1[df1['鄉鎮市區'] == '豐原區'].tail()


# In[5]:


# 驗證桃園市的資料
df1[df1['鄉鎮市區'] == '中壢區'].tail()


# In[6]:


# 驗證新北市的資料
df1[df1['鄉鎮市區'] == '中和區'].tail()


# In[7]:


# 驗證高雄市的資料
df1[df1['鄉鎮市區'] == '小港區'].tail()


# In[8]:


# 驗證台北市的資料
df1[df1['鄉鎮市區'] == '大安區'].tail()


# In[9]:


# Columns
df1.columns


# In[10]:


# 觀察值
df1['主要用途'].unique()


# In[11]:


# 觀察值
df1['建物型態'].unique()


# In[12]:


# 觀察值
df1['總樓層數'].unique()


# In[13]:


# 因為樓層總數為中文
# 欲將其轉成數字 
# 之後方便篩選
floor = df1['總樓層數'].unique().tolist()


# In[14]:


# 用dict紀錄英文轉數字
floor_dict = {}


# In[15]:


floor_dict[floor[0]] = -1
floor_dict[floor[1]] = 9
floor_dict[floor[2]] = 26
floor_dict[floor[3]] = 0
floor_dict[floor[4]] = 18
floor_dict[floor[5]] = 12
floor_dict[floor[6]] = 4
floor_dict[floor[7]] = 40
floor_dict[floor[8]] = 19
floor_dict[floor[9]] = 22
floor_dict[floor[10]] = 6
floor_dict[floor[11]] = 15
floor_dict[floor[12]] = 28
floor_dict[floor[13]] = 10
floor_dict[floor[14]] = 5
floor_dict[floor[15]] = 33
floor_dict[floor[16]] = 8
floor_dict[floor[17]] = 20
floor_dict[floor[18]] = 27
floor_dict[floor[19]] = 11
floor_dict[floor[20]] = 3
floor_dict[floor[21]] = 14
floor_dict[floor[22]] = 7
floor_dict[floor[23]] = 16
floor_dict[floor[24]] = 2
floor_dict[floor[25]] = 17
floor_dict[floor[26]] = 29
floor_dict[floor[27]] = 23
floor_dict[floor[28]] = 13
floor_dict[floor[29]] = 25
floor_dict[floor[30]] = 41
floor_dict[floor[31]] = 34
floor_dict[floor[32]] = 21
floor_dict[floor[33]] = 24
floor_dict[floor[34]] = 37
floor_dict[floor[35]] = 1
floor_dict[floor[36]] = 32
floor_dict[floor[37]] = 0
floor_dict[floor[38]] = 31
floor_dict[floor[39]] = 43
floor_dict[floor[40]] = 36
floor_dict[floor[41]] = 42
floor_dict[floor[42]] = 30
floor_dict[floor[43]] = 35
floor_dict[floor[44]] = 38
floor_dict[floor[45]] = 46
floor_dict[floor[46]] = 39
floor_dict[floor[47]] = 85
floor_dict[floor[48]] = 50


# In[16]:


# Print
floor_dict


# In[17]:


# 新增欄位
# 樓層數的阿拉伯數字
df1['總樓層數(數字)'] = df1['總樓層數'].map(floor_dict)


# In[18]:


# 轉換型態
df1 = df1.astype({'總樓層數(數字)': int})


# In[19]:


# 篩選條件
df1_result = df1[(df1['主要用途'] == '住家用') & (df1['建物型態'].str.match('^住宅大樓*')== True) & (df1['總樓層數(數字)']>=13)]


# In[20]:


# Print 看是否都高於13樓
df1_result['總樓層數(數字)'].unique()


# In[21]:


# Print dataframe
df1_result


# In[22]:


del df1_result['總樓層數(數字)']


# In[23]:


df1_result


# In[24]:


# 重設 index
df1_result = df1_result.reset_index(drop=True)


# In[25]:


df1_result


# In[26]:


# 交易年月日要改成西洋
df1_result['交易年月日'] = df1_result['交易年月日'].astype(str)


# In[27]:


# 擷取年月日
# 年+1911
df1_result['年'] = df1_result['交易年月日'].str[0:3].astype(int)+1911
df1_result['月'] = df1_result['交易年月日'].str[3:5]
df1_result['日'] = df1_result['交易年月日'].str[5:]


# In[28]:


df1_result['交易年月日_國曆'] = df1_result['年'].astype(str)+df1_result['月']+df1_result['日']


# In[29]:


#　改成datetime型態
df1_result['交易年月日_國曆'] = pd.to_datetime(df1_result['交易年月日_國曆'], format='%Y-%m-%d')


# In[30]:


df1_result['交易年月日'] = df1_result['交易年月日_國曆']


# In[31]:


df1_result['交易年月日']


# In[32]:


del df1_result['交易年月日_國曆']
del df1_result['年']
del df1_result['月']
del df1_result['日']


# In[33]:


# 取除各地縣市
df1_result["city"] = df1_result['土地位置建物門牌'].str[0:3]


# In[34]:


df1_result["city"].unique()


# In[35]:


df1_result


# In[36]:


# 按造時間排序 
# 由進到遠
final_result = df1_result.sort_values(by='交易年月日', ascending=False)


# In[37]:


# Print dataframe
final_result


# In[38]:


final_result.columns


# In[39]:


final_result = final_result[["鄉鎮市區", "交易年月日", '建物型態', 'city']]


# In[40]:


# 改欄位名稱
final_result.rename(columns={"鄉鎮市區": "district", "交易年月日": "date", "建物型態": "building_state"}, inplace=True)


# In[41]:


# Convert to pyspark 的 dataframe
sparkDF=spark.createDataFrame(final_result) 
sparkDF.printSchema()
sparkDF.show()


# In[42]:


# 將兩個欄位合併成json格式
df_sparkDataframe = sparkDF.select("city","date",to_json(struct("building_state","district")).alias("events"))


# In[43]:


# Print dataframe
df_sparkDataframe.show(truncate=100)


# In[44]:


# 再合併成 time_slots
df_sparkDataframe = df_sparkDataframe.select("city",to_json(struct("date","events")).alias("time_slots"))


# In[45]:


# Print dataframe
df_sparkDataframe.show(truncate=100)


# In[46]:


# 依照各縣市合併
df_result_sparkDataframe = df_sparkDataframe.groupBy('city').agg(F.collect_list('time_slots').alias('time_slots'))


# In[47]:


# Print dataframe
df_result_sparkDataframe.show(truncate=100)


# In[48]:


# 分成兩個dataframe
df_slice_1, df_slice_2 = df_result_sparkDataframe.randomSplit([0.40, 0.60],seed=1234)


# In[49]:


# Print dataframe
df_slice_1.show(truncate=100)


# In[50]:


# Print dataframe
df_slice_2.show(truncate=100)


# In[51]:


# Output dataframe
df_slice_1.write.json('./result-part1.json')


# In[52]:


# Output dataframe
df_slice_2.write.json('./result-part2.json')

