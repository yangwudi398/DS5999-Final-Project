#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import MinMaxScaler

# Import the F4-version tokens and vocabulary
tokens = pd.read_csv("../tokens_f4.csv", index_col=[0,1,2])
vocab = pd.read_csv("../vocab_f4.csv", index_col=0)

# Select 5000 tokens with the highest tfidf scores, except for the highest one "â"
top_vocab = vocab.sort_values(by="tfidf_mean", ascending=False).iloc[1:5001]

# Select the words
word_index = tokens["term_id"].isin(top_vocab.index)

# Create a bag-of-word model that count each token by documents
BOW = tokens[word_index].groupby(["doc_num", "term_id"])["term_id"].count().to_frame()

# Create a document-term matrix
DTM = BOW.unstack(fill_value=0)

# Compute the TFIDF table
num_docs = DTM.shape[0]
alpha = 0.000001 # arbitrary smoothing value
alpha_sum = alpha * num_docs
TF = DTM.apply(lambda x: pd.to_numeric((x + alpha) / (x.sum() + alpha_sum), downcast="float"), axis=1)
DF = DTM[DTM > 0].count()
TFIDF = TF * np.log2(num_docs / DF)

# Normalize the TFIDF table
min_max_scaler = MinMaxScaler()
TFIDF_norm = min_max_scaler.fit_transform(TFIDF)
TFIDF = pd.DataFrame(TFIDF_norm)
TFIDF.index.names = ["doc_num"]

# Compute PCA table
pca_model = PCA(n_components=10)
projected = pca_model.fit_transform(TFIDF)
pca = pd.DataFrame(projected)
pca.index.names = ["doc_num"]

# Export the PCA table
pca.to_csv("pca.csv", sep=',', index=True)
