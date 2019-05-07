#!/usr/bin/env python
# coding: utf-8

# This Python file adds a POS tag to each token in the tokenized NYTimes documents
# It also adds stop-word dummies and stems to the NYTimes vocabulary

import pandas as pd
import numpy as np
from nltk import pos_tag
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer

# Import NYTimes tokens and vocabulary
tokens = pd.read_csv("tokenized_nyt.csv", index_col=[0,1,2])
vocab = pd.read_csv("vocab_nyt.csv", index_col=0)

# Adjust data types of tokens and terms
tokens["token_str"] = tokens["token_str"].astype("str")
tokens["term_str"] = tokens["term_str"].astype("str")
vocab["term_str"] = vocab["term_str"].astype("str")

# Add stop-word dummies to vocabulary
sw = pd.DataFrame(stopwords.words('english'), columns=['term_str'])
sw = sw.reset_index().set_index('term_str')
sw.columns = ['dummy']
sw.dummy = 1
vocab["stop"] = vocab["term_str"].map(sw["dummy"])
vocab["stop"] = vocab["stop"].fillna(0).astype("int")

# Add stems to vocabulary
stemmer = PorterStemmer()
vocab["stem"] = vocab["term_str"].astype("str").apply(stemmer.stem)

# Generate part-of-speech tags for each token in a sentence
def get_pos(sent):
    try:
        pos_list = pos_tag(sent)
    except:
        pos_list = [("~","~")]
    pos = pd.Series(pos_list).to_frame()
    return pos

# Generate the part-of-speeches tags for the tokenized NYTimes documents
POS = tokens.groupby(["doc_num", "sent_num"]).token_str.apply(get_pos)
POS.index.names = ["doc_num", "sent_num", "token_num"]
POS.columns = ["pos"]

# Add POS tags to the tokens table
tokens.loc[tokens["punc"]==0, "pos_tuple"] = POS["pos"]
tokens.loc[tokens["punc"]==0, "pos"] = POS["pos"].apply(lambda x: x[1])

# Export the new vocab and tokens data frames
tokens.to_csv("tokens_nltk.csv", index=True)
vocab.to_csv("vocab_nltk.csv", index=True)
