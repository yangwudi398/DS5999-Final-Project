#!/usr/bin/env python
# coding: utf-8

import sqlite3
import pandas as pd

# Connect the topic modeling database
topic_model_db = "myproject/topic modeling.db"
topic_model_conn = sqlite3.connect(topic_model_db)

# Get the top 10 keywords for each topic
topic_words = pd.read_sql("SELECT topic_id, topic_words FROM topic", topic_model_conn)
topic_words = topic_words["topic_words"].str.split(" ", n=9, expand=True)
topic_words.index.names = ["topic_id"]
topic_words.columns = ["word 1","word 2","word 3","word 4","word 5","word 6","word 7","word 8","word 9","word 10"]
topic_words.to_csv("topic_words.csv", index=True)

# Get the topic weights for each document
doc_topics = pd.read_sql("SELECT doc_id, topic_id, topic_weight FROM doctopic", topic_model_conn)
doc_topics = doc_topics.sort_values(by=["doc_id","topic_id"]).reset_index(drop=True)
doc_topics.to_csv("doc_topics.csv", index=False)

# Add the datetime and month to each document
doc_dates = pd.read_sql("SELECT doc_id, doc_label FROM doc", topic_model_conn)
doc_dates = doc_dates.set_index("doc_id")
doc_topics["date"] = doc_topics["doc_id"].apply(lambda doc_id: doc_dates.loc[doc_id][0])
doc_topics["month"] = doc_topics["date"].apply(lambda date: date[0:7])

# Average the topic weights of all documents in each day
date_topics = doc_topics.drop("doc_id", axis=1).groupby(["date", "topic_id"]).mean()
date_topics.to_csv("date_topics.csv", index=True)

# Average the topic weights of all documents in each month
month_topics = doc_topics.drop("doc_id", axis=1).groupby(["month", "topic_id"]).mean()
month_topics.to_csv("month_topics.csv", index=True)
