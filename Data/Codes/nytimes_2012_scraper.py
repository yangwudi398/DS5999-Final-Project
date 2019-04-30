#!/usr/bin/env python
# coding: utf-8

# Scrap the content for NYTimes news articles about Middle East in 2012

import re
import sqlite3
import requests
import pandas as pd
from lxml import html

# Import the news database
nytimes_db = "../nytimes.db"
conn = sqlite3.connect(nytimes_db)
nyt_2012 = pd.read_sql("SELECT * FROM middle_east_2012", conn)
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
    
    # Retrieve title
    title = tree.xpath("//head//title/text()")
    title = title[0][:-21]
    
    # Retrieve content
    content_list = tree.xpath("//p[@class='story-body-text story-content']/text()")
    content = " ".join(content_list)
    content = re.sub("â\x80.", "", content)
    
    return title, content

def get_content(df_data):
    # Create a table to save content for each article
    df_content = pd.DataFrame(columns = ["date", "title", "content"])
    
    # For each day
    for i in df_data.index:
        # Get the date
        date = df_data.loc[i, "date"]
        print(date) # Show the current date
        
        # Get the urls of new in that day
        urls = df_data.loc[i, "urls"]
        if len(urls) > 0:
            urls= urls.split("\n")
        else:
            urls = []
        
        # For every news article of that day
        for j in range(len(urls)):
            url = "http://" + urls[j]
            
            print(url) # Show the current url
            title, content = get_page_content(url)
            
            # Append the title, url and content of a news to the content table
            df_content = df_content.append({"date":date, "title":title, "content":content}, ignore_index=True)
    
    # Return the content table        
    return df_content

# Get content for news in January 2012
def in_jan(date):
    return date < "2012-02-01"

jan_2012 = nyt_2012[nyt_2012["date"].apply(in_jan)]
jan_2012_content = get_content(jan_2012)

# Get content for news in Feburary 2012
def in_feb(date):
    return date >= "2012-02-01" and date < "2012-03-01"

feb_2012 = nyt_2012[nyt_2012["date"].apply(in_feb)]
feb_2012_content = get_content(feb_2012)

# Get content for news in March 2012
def in_mar(date):
    return date >= "2012-03-01" and date < "2012-04-01"

mar_2012 = nyt_2012[nyt_2012["date"].apply(in_mar)]
mar_2012_content = get_content(mar_2012)

# Get content for news in April 2012
def in_apr(date):
    return date >= "2012-04-01" and date < "2012-05-01"

apr_2012 = nyt_2012[nyt_2012["date"].apply(in_apr)]
apr_2012_content = get_content(apr_2012)

# Get content for news in May 2012
def in_may(date):
    return date >= "2012-05-01" and date < "2012-06-01"

may_2012 = nyt_2012[nyt_2012["date"].apply(in_may)]
may_2012_content = get_content(may_2012)

# Get content for news in June 2012
def in_jun(date):
    return date >= "2012-06-01" and date < "2012-07-01"

jun_2012 = nyt_2012[nyt_2012["date"].apply(in_jun)]
jun_2012_content = get_content(jun_2012)

# Get content for news in July 2012
def in_jul(date):
    return date >= "2012-07-01" and date < "2012-08-01"

jul_2012 = nyt_2012[nyt_2012["date"].apply(in_jul)]
jul_2012_content = get_content(jul_2012)

# Get content for news in August 2012
def in_aug(date):
    return date >= "2012-08-01" and date < "2012-09-01"

aug_2012 = nyt_2012[nyt_2012["date"].apply(in_aug)]
aug_2012_content = get_content(aug_2012)

# Get content for news in September 2012
def in_sep(date):
    return date >= "2012-09-01" and date < "2012-10-01"

sep_2012 = nyt_2012[nyt_2012["date"].apply(in_sep)]
sep_2012_content = get_content(sep_2012)

# Get content for news in October 2012
def in_oct(date):
    return date >= "2012-10-01" and date < "2012-11-01"

oct_2012 = nyt_2012[nyt_2012["date"].apply(in_oct)]
oct_2012_content = get_content(oct_2012)

# Get content for news in November 2012
def in_nov(date):
    return date >= "2012-11-01" and date < "2012-12-01"

nov_2012 = nyt_2012[nyt_2012["date"].apply(in_nov)]
nov_2012_content = get_content(nov_2012)

# Get content for news in December 2012
def in_dec(date):
    return date >= "2012-12-01"

dec_2012 = nyt_2012[nyt_2012["date"].apply(in_dec)]
dec_2012_content = get_content(dec_2012)

# Export the news content in 2012
nytimes_2012_db = "../nytimes_2012.db"
conn = sqlite3.connect(nytimes_2012_db)

jan_2012_content.to_sql("2012_01", conn, if_exists="replace", index=False)
feb_2012_content.to_sql("2012_02", conn, if_exists="replace", index=False)
mar_2012_content.to_sql("2012_03", conn, if_exists="replace", index=False)
apr_2012_content.to_sql("2012_04", conn, if_exists="replace", index=False)
may_2012_content.to_sql("2012_05", conn, if_exists="replace", index=False)
jun_2012_content.to_sql("2012_06", conn, if_exists="replace", index=False)
jul_2012_content.to_sql("2012_07", conn, if_exists="replace", index=False)
aug_2012_content.to_sql("2012_08", conn, if_exists="replace", index=False)
sep_2012_content.to_sql("2012_09", conn, if_exists="replace", index=False)
oct_2012_content.to_sql("2012_10", conn, if_exists="replace", index=False)
nov_2012_content.to_sql("2012_11", conn, if_exists="replace", index=False)
dec_2012_content.to_sql("2012_12", conn, if_exists="replace", index=False)

conn.close()
