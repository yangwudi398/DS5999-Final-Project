#!/usr/bin/env python
# coding: utf-8

# This python file tokenize the NYTimes documents,
# and create a vocabulary table for the tokenized document

import sqlite3
import pandas as pd
import numpy as np

# Paths to all NYTimes news databases
nyt_2011_db = "../Data/nytimes_2011.db"
nyt_2012_db = "../Data/nytimes_2012.db"
nyt_2013_db = "../Data/nytimes_2013.db"
nyt_2014_db = "../Data/nytimes_2014.db"
nyt_2015_db = "../Data/nytimes_2015.db"
nyt_2016_db = "../Data/nytimes_2016.db"
nyt_2017_db = "../Data/nytimes_2017.db"
nyt_2018_db = "../Data/nytimes_2018.db"

# Get the news documents for 2011
nyt_2011_conn = sqlite3.connect(nyt_2011_db)
nyt_2011_01 = pd.read_sql("SELECT * FROM '2011_01'", nyt_2011_conn)
nyt_2011 = nyt_2011_01.copy()
for i in range(2, 13):
    sql = "SELECT * FROM '2011_" + "{:02d}".format(i) + "'"
    df = pd.read_sql(sql, nyt_2011_conn)
    nyt_2011 = nyt_2011.append(df)
nyt_2011_conn.close()
nyt_2011 = nyt_2011.reset_index(drop=True)

# Get the news documents for 2012
nyt_2012_conn = sqlite3.connect(nyt_2012_db)
nyt_2012_01 = pd.read_sql("SELECT * FROM '2012_01'", nyt_2012_conn)
nyt_2012 = nyt_2012_01.copy()
for i in range(2, 13):
    sql = "SELECT * FROM '2012_" + "{:02d}".format(i) + "'"
    df = pd.read_sql(sql, nyt_2012_conn)
    nyt_2012 = nyt_2012.append(df)
nyt_2012_conn.close()
nyt_2012 = nyt_2012.reset_index(drop=True)

# Get the news documents for 2013
nyt_2013_conn = sqlite3.connect(nyt_2013_db)
nyt_2013_01 = pd.read_sql("SELECT * FROM '2013_01'", nyt_2013_conn)
nyt_2013 = nyt_2013_01.copy()
for i in range(2, 13):
    sql = "SELECT * FROM '2013_" + "{:02d}".format(i) + "'"
    df = pd.read_sql(sql, nyt_2013_conn)
    nyt_2013 = nyt_2013.append(df)
nyt_2013_conn.close()
nyt_2013 = nyt_2013.reset_index(drop=True)

# Get the news documents for 2014
nyt_2014_conn = sqlite3.connect(nyt_2014_db)
nyt_2014_01 = pd.read_sql("SELECT * FROM '2014_01'", nyt_2014_conn)
nyt_2014 = nyt_2014_01.copy()
for i in range(2, 13):
    sql = "SELECT * FROM '2014_" + "{:02d}".format(i) + "'"
    df = pd.read_sql(sql, nyt_2014_conn)
    nyt_2014 = nyt_2014.append(df)
nyt_2014_conn.close()
nyt_2014 = nyt_2014.reset_index(drop=True)

# Get the news documents for 2015
nyt_2015_conn = sqlite3.connect(nyt_2015_db)
nyt_2015_01 = pd.read_sql("SELECT * FROM '2015_01'", nyt_2015_conn)
nyt_2015 = nyt_2015_01.copy()
for i in range(2, 13):
    sql = "SELECT * FROM '2015_" + "{:02d}".format(i) + "'"
    df = pd.read_sql(sql, nyt_2015_conn)
    nyt_2015 = nyt_2015.append(df)
nyt_2015_conn.close()
nyt_2015 = nyt_2015.reset_index(drop=True)

# Get the news documents for 2016
nyt_2016_conn = sqlite3.connect(nyt_2016_db)
nyt_2016_01 = pd.read_sql("SELECT * FROM '2016_01'", nyt_2016_conn)
nyt_2016 = nyt_2016_01.copy()
for i in range(2, 13):
    sql = "SELECT * FROM '2016_" + "{:02d}".format(i) + "'"
    df = pd.read_sql(sql, nyt_2016_conn)
    nyt_2016 = nyt_2016.append(df)
nyt_2016_conn.close()
nyt_2016 = nyt_2016.reset_index(drop=True)

# Get the news documents for 2017
nyt_2017_conn = sqlite3.connect(nyt_2017_db)
nyt_2017_01 = pd.read_sql("SELECT * FROM '2017_01'", nyt_2017_conn)
nyt_2017 = nyt_2017_01.copy()
for i in range(2, 13):
    sql = "SELECT * FROM '2017_" + "{:02d}".format(i) + "'"
    df = pd.read_sql(sql, nyt_2017_conn)
    nyt_2017 = nyt_2017.append(df)
nyt_2017_conn.close()
nyt_2017 = nyt_2017.reset_index(drop=True)

# Get the news documents for 2018
nyt_2018_conn = sqlite3.connect(nyt_2018_db)
nyt_2018_01 = pd.read_sql("SELECT * FROM '2018_01'", nyt_2018_conn)
nyt_2018 = nyt_2018_01.copy()
for i in range(2, 13):
    sql = "SELECT * FROM '2018_" + "{:02d}".format(i) + "'"
    df = pd.read_sql(sql, nyt_2018_conn)
    nyt_2018 = nyt_2018.append(df)
nyt_2018_conn.close()
nyt_2018 = nyt_2018.reset_index(drop=True)

# Get a single data frame for all NYTimes news documents
nyt = nyt_2011.append(nyt_2012).append(nyt_2013).append(nyt_2014).      append(nyt_2015).append(nyt_2016).append(nyt_2017).append(nyt_2018)
nyt = nyt.reset_index(drop=True)

# Tokenize the NYTimes documents
def tokenize_nyt(df):
    # Split each document into paragraphs
    para_regex = r"(?<![(Mr)(MR)(Mrs)(Ms)A-Z])[.;?!]\s"
    paras = df.content.str.split(para_regex, expand=True).stack().to_frame("sent_str")
    paras.index.names = ["doc_num", "sent_num"]
    
    # Split each paragraph into tokens
    token_regex = r"\W+"
    tokens = paras.sent_str.str.split(token_regex, expand=True).stack().to_frame("token_str")
    tokens.index.names = ["doc_num", "sent_num", "token_num"]
    
    # Tag punction
    tokens["punc"] = tokens.token_str.str.match(r"^\W*$").astype('int')
    
    # Get the lowercase term for each token
    tokens.loc[tokens.punc==0, "term_str"] = tokens.token_str.str.lower()
    
    return tokens

tokens = tokenize_nyt(nyt)

# Generate the vocabulary for all NYTimes documents
def generate_vocab(tokens):
    vocab = tokens[tokens.punc==0].term_str.value_counts().to_frame().reset_index()
    vocab = vocab.rename(columns={"index":"term_str", "term_str":"count"})
    vocab.index.name = "term_id"
    return vocab

vocab = generate_vocab(tokens)

# Retrieve term id by term string
def get_term_id(term_str):
    try:
        term_id = vocab[vocab["term_str"]==term_str].index[0]
    except:
        term_id = None   
    return term_id

# Retrieve term string by term id
def get_term_str(term_id):
    try:
        term_id = vocab.loc[term_id].term_str
    except:
        term_id = None
    return term_id

# Get term id for terms in the NYTimes tokens
tokens["term_id"] = tokens["term_str"].map(vocab.reset_index().set_index('term_str').term_id)
tokens["term_id"] = tokens["term_id"].fillna(-1).astype('int')

# Export the tokens and vocabs tables to csv files
tokens.to_csv("tokenized_nyt.csv", index=True)
vocab.to_csv("vocab_nyt.csv", index=True)
