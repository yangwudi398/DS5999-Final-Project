#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re
import sqlite3
import pandas as pd


# In[2]:


conn = sqlite3.connect("nytimes.db")
df_2018 = pd.read_sql("SELECT * FROM middle_east_2018", conn)
df_2017 = pd.read_sql("SELECT * FROM middle_east_2017", conn)
df_2016 = pd.read_sql("SELECT * FROM middle_east_2016", conn)
df_2015 = pd.read_sql("SELECT * FROM middle_east_2015", conn)
df_2014 = pd.read_sql("SELECT * FROM middle_east_2014", conn)
df_2013 = pd.read_sql("SELECT * FROM middle_east_2013", conn)
df_2012 = pd.read_sql("SELECT * FROM middle_east_2012", conn)
df_2011 = pd.read_sql("SELECT * FROM middle_east_2011", conn)


# In[3]:


def date_format(date):
    return date[0:10]

df_2018["date"] = df_2018["date"].apply(date_format)
df_2017["date"] = df_2017["date"].apply(date_format)
df_2016["date"] = df_2016["date"].apply(date_format)
df_2015["date"] = df_2015["date"].apply(date_format)
df_2014["date"] = df_2014["date"].apply(date_format)
df_2013["date"] = df_2013["date"].apply(date_format)
df_2012["date"] = df_2012["date"].apply(date_format)
df_2011["date"] = df_2011["date"].apply(date_format)


# In[4]:


def split_list(lst):
    lst = lst[1:-1]
    lst = lst.split(", ")
    lst = [e[1:-1] for e in lst]
    
    list_string = ""
    for item in lst:
        list_string += item + "\n"
    
    return list_string[:-1]


# In[5]:


df_2018["titles"] = df_2018["titles"].apply(split_list)
df_2017["titles"] = df_2017["titles"].apply(split_list)
df_2016["titles"] = df_2016["titles"].apply(split_list)
df_2015["titles"] = df_2015["titles"].apply(split_list)
df_2014["titles"] = df_2014["titles"].apply(split_list)
df_2013["titles"] = df_2013["titles"].apply(split_list)
df_2012["titles"] = df_2012["titles"].apply(split_list)
df_2011["titles"] = df_2011["titles"].apply(split_list)


# In[6]:


df_2018["urls"] = df_2018["urls"].apply(split_list)
df_2017["urls"] = df_2017["urls"].apply(split_list)
df_2016["urls"] = df_2016["urls"].apply(split_list)
df_2015["urls"] = df_2015["urls"].apply(split_list)
df_2014["urls"] = df_2014["urls"].apply(split_list)
df_2013["urls"] = df_2013["urls"].apply(split_list)
df_2012["urls"] = df_2012["urls"].apply(split_list)
df_2011["urls"] = df_2011["urls"].apply(split_list)


# In[7]:


df_2018.to_sql("middle_east_2018", conn, if_exists="replace", index=False)
df_2017.to_sql("middle_east_2017", conn, if_exists="replace", index=False)
df_2016.to_sql("middle_east_2016", conn, if_exists="replace", index=False)
df_2015.to_sql("middle_east_2015", conn, if_exists="replace", index=False)
df_2014.to_sql("middle_east_2014", conn, if_exists="replace", index=False)
df_2013.to_sql("middle_east_2013", conn, if_exists="replace", index=False)
df_2012.to_sql("middle_east_2012", conn, if_exists="replace", index=False)
df_2011.to_sql("middle_east_2011", conn, if_exists="replace", index=False)


# In[8]:


conn.close()


# In[ ]:




