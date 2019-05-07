# Text Representation
This directory contains the python program used to create F4 version of the text.

"01_tokenize_nytimes.py" creates a table that contain all tokens for each NYTimes News document,
and a vocabulary table that store all tokens mentioned in those documents.
It also connects the document-token with the vocabulary table with a unique term id for each token.

"02_nltk_annotation.py" adds a part-of-speech tag to each tokens in the document-token table,
and also adds stop-word dummies and stem for each token in the vocabulary table.

"03_tf-idf.py" creates a bag-of-word model with which to compute the tf, tf-idf, and tf-th features 
for each token in the vocabulary table.
