#!/usr/bin/env python
# coding: utf-8

# Scrap the content for NYTimes news articles about Middle East in 2015

import re
import sqlite3
import requests
import pandas as pd
from lxml import html

# Import the news database
nytimes_db = "nytimes.db"
conn = sqlite3.connect(../nytimes_db)
nyt_2015 = pd.read_sql("SELECT * FROM middle_east_2015", conn)
conn.close()

# Methods for Scraping the news content with an url
def get_page_content(url):
    # Load a web page
    try:
        page = requests.get(url)
    except: # Reload if a request fails
        page = requests.get(url)
    
    # Constitute a tree
    tree = html.fromstring(page.content)
    
    # Retrieve content
    content_list = tree.xpath("//section[@name='articleBody']//p/text()")
    content = " ".join(content_list)
    
    return content

def get_content(df_data):
    # Create a table to save content for each article
    df_content = pd.DataFrame(columns = ["date", "title", "content"])
    
    # For each day
    for i in df_data.index:
        # Get the date
        date = df_data.loc[i, "date"]
        print(date) # Show the current date
        
        # Get the titles of news in that day
        titles = df_data.loc[i, "titles"]
        if len(titles) > 0: 
            titles = titles.split("\n")
        else:
            titles = []
        
        # Get the urls of new in that day
        urls = df_data.loc[i, "urls"]
        if len(urls) > 0:
            urls= urls.split("\n")
        else:
            urls = []
        
        # For every news article of that day
        for j in range(len(titles)):
            title = re.sub("-", " ", titles[j])
            url = "http://" + urls[j]
            
            print(url) # Show the current url
            content = get_page_content(url)
            
            # Append the title, url and content of a news to the content table
            df_content = df_content.append({"date":date, "title":title, "content":content}, ignore_index=True)
    
    # Return the content table        
    return df_content


# Get content for news in January 2015
def in_jan(date):
    return date < "2015-02-01"

jan_2015 = nyt_2015[nyt_2015["date"].apply(in_jan)]
jan_2015_content = get_content(jan_2015)

# Get content for news in Feburary 2015
def in_feb(date):
    return date >= "2015-02-01" and date < "2015-03-01"

feb_2015 = nyt_2015[nyt_2015["date"].apply(in_feb)]
feb_2015_content = get_content(feb_2015)

# Get content for news in March 2015
def in_mar(date):
    return date >= "2015-03-01" and date < "2015-04-01"

mar_2015 = nyt_2015[nyt_2015["date"].apply(in_mar)]
mar_2015_content = get_content(mar_2015)

# Get content for news in April 2015
def in_apr(date):
    return date >= "2015-04-01" and date < "2015-05-01"

apr_2015 = nyt_2015[nyt_2015["date"].apply(in_apr)]
apr_2015_content = get_content(apr_2015)

# Get content for news in May 2015
def in_may(date):
    return date >= "2015-05-01" and date < "2015-06-01"

may_2015 = nyt_2015[nyt_2015["date"].apply(in_may)]
may_2015_content = get_content(may_2015)

# Get content for news in June 2015
def in_jun(date):
    return date >= "2015-06-01" and date < "2015-07-01"

jun_2015 = nyt_2015[nyt_2015["date"].apply(in_jun)]
jun_2015_content = get_content(jun_2015)

# Get content for news in July 2015
def in_jul(date):
    return date >= "2015-07-01" and date < "2015-08-01"

jul_2015 = nyt_2015[nyt_2015["date"].apply(in_jul)]
jul_2015_content = get_content(jul_2015)

# Get content for news in August 2015
def in_aug(date):
    return date >= "2015-08-01" and date < "2015-09-01"

aug_2015 = nyt_2015[nyt_2015["date"].apply(in_aug)]
aug_2015_content = get_content(aug_2015)

# Get content for news in September 2015
def in_sep(date):
    return date >= "2015-09-01" and date < "2015-10-01"

sep_2015 = nyt_2015[nyt_2015["date"].apply(in_sep)]
sep_2015_content = get_content(sep_2015)

# Get content for news in October 2015
def in_oct(date):
    return date >= "2015-10-01" and date < "2015-11-01"

oct_2015 = nyt_2015[nyt_2015["date"].apply(in_oct)]
oct_2015_content = get_content(oct_2015)

# Get content for news in November 2015
def in_nov(date):
    return date >= "2015-11-01" and date < "2015-12-01"

nov_2015 = nyt_2015[nyt_2015["date"].apply(in_nov)]
nov_2015_content = get_content(nov_2015)

# Get content for news in December 2015
def in_dec(date):
    return date >= "2015-12-01"

dec_2015 = nyt_2015[nyt_2015["date"].apply(in_dec)]
dec_2015_content = get_content(dec_2015)

# Export the news content in 2015
nytimes_2015_db = "../nytimes_2015.db"
conn = sqlite3.connect(nytimes_2015_db)

jan_2015_content.to_sql("2015_01", conn, if_exists="replace", index=False)
feb_2015_content.to_sql("2015_02", conn, if_exists="replace", index=False)
mar_2015_content.to_sql("2015_03", conn, if_exists="replace", index=False)
apr_2015_content.to_sql("2015_04", conn, if_exists="replace", index=False)
may_2015_content.to_sql("2015_05", conn, if_exists="replace", index=False)
jun_2015_content.to_sql("2015_06", conn, if_exists="replace", index=False)
jul_2015_content.to_sql("2015_07", conn, if_exists="replace", index=False)
aug_2015_content.to_sql("2015_08", conn, if_exists="replace", index=False)
sep_2015_content.to_sql("2015_09", conn, if_exists="replace", index=False)
oct_2015_content.to_sql("2015_10", conn, if_exists="replace", index=False)
nov_2015_content.to_sql("2015_11", conn, if_exists="replace", index=False)
dec_2015_content.to_sql("2015_12", conn, if_exists="replace", index=False)

conn.close()


# In[ ]:




