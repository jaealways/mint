from pymongo import MongoClient
import pandas as pd
import numpy as np
from data_transformation.db_env import DbEnv, db
from data_modeling.nlp_modeling import NLPModeling
from bertopic import BERTopic
from datetime import datetime
from sklearn.feature_extraction.text import CountVectorizer
from dateutil.relativedelta import relativedelta
from gensim.models import CoherenceModel
from gensim.corpora import Dictionary


# Data preprocessing
# Raw Document Data
client = MongoClient('localhost', 27017)
db1 = client.music_cow
db2 = client.article
col1 = db1.musicCowData
col5 = db2.article_info

date='2023-02-11'

import re
from bs4 import BeautifulSoup


def text_cleaning(text):
    text = re.sub('<a[ _a-zA-Z=\"\']+href.*?>(.*?)<\/a>', ' ', text)
    text = re.sub('<td[ _a-zA-Z=\"\']+.*?>(.*?)<\/td>', ' ', text)
    text = re.sub('<head.*/head>', ' ', text)
    text = re.sub('&[a-zA-Z0-9]+;', ' ', text)
    text = re.sub('<br />', ' ', text)
    text = BeautifulSoup(text).get_text()
    text = re.sub('[ ]+', ' ', text)
    return text


articles = list(col5.find({'text': {'$exists': True}}))[:20]
title1 = list(map(lambda x: x['article_title'], articles))
text1 = list(map(lambda x: x['text'], articles))
data = list(map(lambda x, y: '%s. %s' % (x, y), title1, text1))
data=list(map(lambda x: text_cleaning(x), data))
data=[x for x in data if x!='. ']

print('data preprocess done', len(data),date)


import torch
from transformers import AutoTokenizer, AutoModel
from kobert_transformers import get_kobert_model

tokenizer = AutoTokenizer.from_pretrained('monologg/kobert')
model = get_kobert_model()
max_len=128

def get_embedding_matrix(documents):
    # Initialize embedding matrix
    embedding_matrix = torch.zeros((len(documents), max_len, 256))

    # Loop through each document
    for i, doc in enumerate(documents):
        # Truncate document to maximum length of 256
        truncated_doc = doc[:max_len]

        # Tokenize document
        tokens = tokenizer.encode_plus(
            truncated_doc,
            add_special_tokens=True,
            max_length=max_len,
            padding='max_length',
            return_tensors='pt'
        )

        # Get model output
        with torch.no_grad():
            output = model(**tokens)

        # Save embedding to matrix
        embedding_matrix[i, :, :] = output.last_hidden_state[:,:, :256]

    return embedding_matrix

embedding_matrix = get_embedding_matrix(data)
print('embedding_matrix done')

# reshape tensor into a 2-dimensional array
X = embedding_matrix.reshape(embedding_matrix.shape[0], -1)

# set the number of clusters to be used in K-means
num_clusters = 2

import gensim
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from sklearn.cluster import KMeans

# assume that your tensor is named 'tensor'

############################k-means######################

# # perform K-means clustering
# kmeans = KMeans(n_clusters=num_clusters, random_state=0).fit(X)
# print('Convert tokenized documents done')
###############################################################


import gensim
from gensim import corpora

# create a dictionary from the feature matrix
dictionary = corpora.Dictionary(data)

# create a corpus from the feature matrix
corpus = [dictionary.doc2bow(doc) for doc in tokenizer]

# set the number of topics to be used in LDA
num_topics = 10

# perform LDA
lda_model = gensim.models.ldamodel.LdaModel(corpus=corpus, num_topics=num_topics)

# print the topics and their top words
for topic in lda_model.print_topics():
    print(topic)




# Perform LDA topic modeling and evaluate perplexity for different number of topics
perplexities = []
models = []
for num_topics in range(2,len(data)**0.5):
    print(num_topics)
    lda_model = gensim.models.ldamodel.LdaModel(corpus=corpus_bow, num_topics=num_topics, id2word=dictionary)
    perplexity = lda_model.log_perplexity(corpus_bow)
    perplexities.append(perplexity)
    models.append(lda_model)

# Plot perplexity vs number of topics
plt.plot(range(2,len(data)**0.5), perplexities)
plt.xlabel('Number of Topics')
plt.ylabel('Perplexity')
plt.show()
plt.savefig('Perplexity.png')

# Get topic distribution for each document using the best model
best_model_index = np.argmin(perplexities)
best_model = models[best_model_index]
topic_distributions = best_model[corpus_bow]

# Convert topic distributions to pandas dataframe
topic_df = pd.DataFrame.from_records([{f'topic_{i}': prob for i, prob in row} for row in topic_distributions])
topic_df.to_pickle("topic_df_%s.pkl" % num_topics)

