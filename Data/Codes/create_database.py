#!/usr/bin/env python
# coding: utf-8

# # Create a Database that Records the titles and urls of NYTimes articles under the Middle-East catagory for each day from 2011/01/01 to 2018/12/31

import re
import sqlite3
import pandas as pd
from urllib import request

# Connect to a databaes
db_path = "../nytimes.db"
conn = sqlite3.connect(db_path)

# My API key for NYTimes.com
key = "q0IcVAOYeeQ9JespP62vuZxE77YY6GhU"

# For each year from 2011 to 2018
for year in range(2011, 2019):
    # Create a dataframe for the year
    df = pd.DataFrame(columns=["date", "titles", "urls"])
    df["date"] = pd.date_range(pd.datetime(year,1,1), pd.datetime(year,12,31))
    
    # For each month in the year
    for m in range(1,13):
        # Get the row indices of the month
        if m != 12:
            index = df.index[(df["date"] >= pd.datetime(year,m,1)) & 
                             (df["date"] < pd.datetime(year,m+1,1))].tolist()
        else:
            index = df.index[(df["date"] >= pd.datetime(year,m,1)) & 
                             (df["date"] <= pd.datetime(year,m,31))].tolist()
        
        # Download the archive information for the month
        url = "https://api.nytimes.com/svc/archive/v1/%d/%d.json?api-key=%s"%(year, m, key)
        response = request.urlopen(url)
        archive = response.read().decode()
        
        # Update the titles and urls information for each day in the month
        for i in index:
            time = df["date"][i]
            year = time.year
            month = "%02d"%m
            day = "%02d"%time.day
        
            # Find the urls of all articles under the catagory "world/middleeast"
            regex = "www.nytimes.com\\\\/%s\\\\/%s\\\\/%s\\\\/world\\\\/middleeast\\\\/[^\"]+"%(year,month,day)
            urls = set(re.findall(regex, archive))
        
            # Get the titles by urls
            urls = [re.sub("\\\\","",url) for url in urls]
            titles = [url[44:-5] for url in urls]
        
            df["titles"][i] = titles
            df["urls"][i] = urls
    
    # Adjust the format of the titles and urls	
    df["titles"] = df["titles"].astype("str")
    df["urls"] = df["urls"].astype("str")
    
    # Export the data frame to a dababase
    table = "middle_east_%d"%year
    df.to_sql(table, conn, if_exists="replace", index=False)

conn.close()
