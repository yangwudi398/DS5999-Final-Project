#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import time

# Import tokens and vocabulary for NYTimes documents
tokens = pd.read_csv("tokens_nltk.csv", index_col=[0,1,2])
vocab = pd.read_csv("vocab_nltk.csv", index_col=0)

# Drop pos_tuple column
tokens = tokens.drop("pos_tuple", axis=1)

# Add dummies for numbers
tokens["num"] = tokens.token_str.str.match(r"^.*\d.*$", na=0).astype("int")

# Filter out punctuations, numbers, and stop words, NA words
word_index = (tokens["punc"] == 0) & (tokens["num"] == 0) &              (tokens["term_str"].isna() == False) &              (tokens["term_id"].isin(vocab[vocab["stop"]==0].index))

# Create a bag-of-word model that count each token by documents
BOW = tokens[word_index].groupby(["doc_num", "term_id"])["term_id"].count().to_frame()

# Create a document-term matrix
DTM = BOW.unstack(fill_value=0)

# Split the data
dtm1 = DTM.iloc[:, 0:10000]
dtm2 = DTM.iloc[:, 10000:20000]
dtm3 = DTM.iloc[:, 20000:30000]
dtm4 = DTM.iloc[:, 30000:40000]
dtm5 = DTM.iloc[:, 40000:50000]
dtm6 = DTM.iloc[:, 50000:60000]
dtm7 = DTM.iloc[:, 60000:]

# Export or delete tables
tokens.to_csv("tokens_f4.csv")
del tokens
del BOW
del DTM


# Compute term frequency tables
alpha = 0.000001 # arbitrary smoothing value
alpha_sum = alpha * dtm1.shape[0]

tf1 = dtm1.apply(lambda x: pd.to_numeric((x + alpha) / (x.sum() + alpha_sum), downcast="float"), axis=1)
tf2 = dtm2.apply(lambda x: pd.to_numeric((x + alpha) / (x.sum() + alpha_sum), downcast="float"), axis=1)
tf3 = dtm3.apply(lambda x: pd.to_numeric((x + alpha) / (x.sum() + alpha_sum), downcast="float"), axis=1)
tf4 = dtm4.apply(lambda x: pd.to_numeric((x + alpha) / (x.sum() + alpha_sum), downcast="float"), axis=1)
tf5 = dtm5.apply(lambda x: pd.to_numeric((x + alpha) / (x.sum() + alpha_sum), downcast="float"), axis=1)
tf6 = dtm6.apply(lambda x: pd.to_numeric((x + alpha) / (x.sum() + alpha_sum), downcast="float"), axis=1)
tf7 = dtm7.apply(lambda x: pd.to_numeric((x + alpha) / (x.sum() + alpha_sum), downcast="float"), axis=1)


# Compute the document frequency tables
df1 = dtm1[dtm1 > 0].count()
df2 = dtm2[dtm2 > 0].count()
df3 = dtm3[dtm3 > 0].count()
df4 = dtm4[dtm4 > 0].count()
df5 = dtm5[dtm5 > 0].count()
df6 = dtm6[dtm6 > 0].count()
df7 = dtm7[dtm7 > 0].count()


# Compute TF-IDF tables
num_docs = dtm1.shape[0]

tfidf1 = tf1 * np.log2(num_docs / df1)
tfidf2 = tf2 * np.log2(num_docs / df2)
tfidf3 = tf3 * np.log2(num_docs / df3)
tfidf4 = tf4 * np.log2(num_docs / df4)
tfidf5 = tf5 * np.log2(num_docs / df5)
tfidf6 = tf6 * np.log2(num_docs / df6)
tfidf7 = tf7 * np.log2(num_docs / df7)

# Delete DTM tables
del dtm1, dtm2, dtm3, dtm4, dtm5, dtm6, dtm7


# Compute TF-TH Tables
thm1 = -(tf1 * np.log2(tf1))
tfth1 = tf1 * thm1.sum()

thm2 = -(tf2 * np.log2(tf2))
tfth2 = tf2 * thm2.sum()

thm3 = -(tf3 * np.log2(tf3))
tfth3 = tf3 * thm3.sum()

thm4 = -(tf4 * np.log2(tf4))
tfth4 = tf4 * thm4.sum()

thm5 = -(tf5 * np.log2(tf5))
tfth5 = tf5 * thm5.sum()

thm6 = -(tf6 * np.log2(tf6))
tfth6 = tf6 * thm6.sum()

thm7 = -(tf7 * np.log2(tf7))
tfth7 = tf7 * thm7.sum()


# TF Sums
tf_sum1 = tf1.sum()
tf_sum1.index = tf_sum1.index.droplevel(0)

tf_sum2 = tf2.sum()
tf_sum2.index = tf_sum2.index.droplevel(0)

tf_sum3 = tf3.sum()
tf_sum3.index = tf_sum3.index.droplevel(0)

tf_sum4 = tf4.sum()
tf_sum4.index = tf_sum4.index.droplevel(0)

tf_sum5 = tf5.sum()
tf_sum5.index = tf_sum5.index.droplevel(0)

tf_sum6 = tf6.sum()
tf_sum6.index = tf_sum6.index.droplevel(0)

tf_sum7 = tf7.sum()
tf_sum7.index = tf_sum7.index.droplevel(0)

tf_sum = tf_sum1.append(tf_sum2).append(tf_sum3).append(tf_sum4).         append(tf_sum5).append(tf_sum6).append(tf_sum7)
del tf_sum1, tf_sum2, tf_sum3, tf_sum4, tf_sum5, tf_sum6, tf_sum7

vocab["tf_sum"] = tf_sum
vocab["tf_sum"] = vocab["tf_sum"].fillna(0)
del tf_sum


# TF Means
vocab["tf_mean"] = vocab["tf_sum"] / num_docs


# TF Maximums
tf_max1 = tf1.max()
tf_max1.index = tf_max1.index.droplevel(0)

tf_max2 = tf2.max()
tf_max2.index = tf_max2.index.droplevel(0)

tf_max3 = tf3.max()
tf_max3.index = tf_max3.index.droplevel(0)

tf_max4 = tf4.max()
tf_max4.index = tf_max4.index.droplevel(0)

tf_max5 = tf5.max()
tf_max5.index = tf_max5.index.droplevel(0)

tf_max6 = tf6.max()
tf_max6.index = tf_max6.index.droplevel(0)

tf_max7 = tf7.max()
tf_max7.index = tf_max7.index.droplevel(0)

tf_max = tf_max1.append(tf_max2).append(tf_max3).append(tf_max4).         append(tf_max5).append(tf_max6).append(tf_max7)
del tf_max1, tf_max2, tf_max3, tf_max4, tf_max5, tf_max6, tf_max7

vocab["tf_max"] = tf_max
vocab["tf_max"] = vocab["tf_max"].fillna(0)
del tf_max

# Delete TF tables
del tf1, tf2, tf3, tf4, tf5, tf6, tf7


# TF-IDF Sums
tfidf_sum1 = tfidf1.sum()
tfidf_sum1.index = tfidf_sum1.index.droplevel(0)

tfidf_sum2 = tfidf2.sum()
tfidf_sum2.index = tfidf_sum2.index.droplevel(0)

tfidf_sum3 = tfidf3.sum()
tfidf_sum3.index = tfidf_sum3.index.droplevel(0)

tfidf_sum4 = tfidf4.sum()
tfidf_sum4.index = tfidf_sum4.index.droplevel(0)

tfidf_sum5 = tfidf5.sum()
tfidf_sum5.index = tfidf_sum5.index.droplevel(0)

tfidf_sum6 = tfidf6.sum()
tfidf_sum6.index = tfidf_sum6.index.droplevel(0)

tfidf_sum7 = tfidf7.sum()
tfidf_sum7.index = tfidf_sum7.index.droplevel(0)

tfidf_sum = tfidf_sum1.append(tfidf_sum2).append(tfidf_sum3).append(tfidf_sum4).         append(tfidf_sum5).append(tfidf_sum6).append(tfidf_sum7)
del tfidf_sum1, tfidf_sum2, tfidf_sum3, tfidf_sum4, tfidf_sum5, tfidf_sum6, tfidf_sum7

vocab["tfidf_sum"] = tfidf_sum
vocab["tfidf_sum"] = vocab["tfidf_sum"].fillna(0)
del tfidf_sum


# TFIDF Means
vocab["tfidf_mean"] = vocab["tfidf_sum"] / num_docs


# TFIDF Maximums
tfidf_max1 = tfidf1.max()
tfidf_max1.index = tfidf_max1.index.droplevel(0)

tfidf_max2 = tfidf2.max()
tfidf_max2.index = tfidf_max2.index.droplevel(0)

tfidf_max3 = tfidf3.max()
tfidf_max3.index = tfidf_max3.index.droplevel(0)

tfidf_max4 = tfidf4.max()
tfidf_max4.index = tfidf_max4.index.droplevel(0)

tfidf_max5 = tfidf5.max()
tfidf_max5.index = tfidf_max5.index.droplevel(0)

tfidf_max6 = tfidf6.max()
tfidf_max6.index = tfidf_max6.index.droplevel(0)

tfidf_max7 = tfidf7.max()
tfidf_max7.index = tfidf_max7.index.droplevel(0)

tfidf_max = tfidf_max1.append(tfidf_max2).append(tfidf_max3).append(tfidf_max4).         append(tfidf_max5).append(tfidf_max6).append(tfidf_max7)
del tfidf_max1, tfidf_max2, tfidf_max3, tfidf_max4, tfidf_max5, tfidf_max6, tfidf_max7

vocab["tfidf_max"] = tfidf_max
vocab["tfidf_max"] = vocab["tfidf_max"].fillna(0)
del tfidf_max

# Delete TFIDF tables
del tfidf1, tfidf2, tfidf3, tfidf4, tfidf5, tfidf6, tfidf7


# TFTH Sums
tfth_sum1 = tfth1.sum()
tfth_sum1.index = tfth_sum1.index.droplevel(0)

tfth_sum2 = tfth2.sum()
tfth_sum2.index = tfth_sum2.index.droplevel(0)

tfth_sum3 = tfth3.sum()
tfth_sum3.index = tfth_sum3.index.droplevel(0)

tfth_sum4 = tfth4.sum()
tfth_sum4.index = tfth_sum4.index.droplevel(0)

tfth_sum5 = tfth5.sum()
tfth_sum5.index = tfth_sum5.index.droplevel(0)

tfth_sum6 = tfth6.sum()
tfth_sum6.index = tfth_sum6.index.droplevel(0)

tfth_sum7 = tfth7.sum()
tfth_sum7.index = tfth_sum7.index.droplevel(0)

tfth_sum = tfth_sum1.append(tfth_sum2).append(tfth_sum3).append(tfth_sum4).         append(tfth_sum5).append(tfth_sum6).append(tfth_sum7)
del tfth_sum1, tfth_sum2, tfth_sum3, tfth_sum4, tfth_sum5, tfth_sum6, tfth_sum7

vocab["tfth_sum"] = tfth_sum
vocab["tfth_sum"] = vocab["tfth_sum"].fillna(0)
del tfth_sum


# TFTH Means
vocab["tfth_mean"] = vocab["tfth_sum"] / num_docs


# TFTH Maximums
tfth_max1 = tfth1.max()
tfth_max1.index = tfth_max1.index.droplevel(0)

tfth_max2 = tfth2.max()
tfth_max2.index = tfth_max2.index.droplevel(0)

tfth_max3 = tfth3.max()
tfth_max3.index = tfth_max3.index.droplevel(0)

tfth_max4 = tfth4.max()
tfth_max4.index = tfth_max4.index.droplevel(0)

tfth_max5 = tfth5.max()
tfth_max5.index = tfth_max5.index.droplevel(0)

tfth_max6 = tfth6.max()
tfth_max6.index = tfth_max6.index.droplevel(0)

tfth_max7 = tfth7.max()
tfth_max7.index = tfth_max7.index.droplevel(0)

tfth_max = tfth_max1.append(tfth_max2).append(tfth_max3).append(tfth_max4).         append(tfth_max5).append(tfth_max6).append(tfth_max7)
del tfth_max1, tfth_max2, tfth_max3, tfth_max4, tfth_max5, tfth_max6, tfth_max7

vocab["tfth_max"] = tfth_max
vocab["tfth_max"] = vocab["tfth_max"].fillna(0)
del tfth_max

# Delete TFTH tables
del tfth1, tfth2, tfth3, tfth4, tfth5, tfth6, tfth7


# Export vocabulary table
vocab.to_csv("vocab_f4.csv", index=True)
