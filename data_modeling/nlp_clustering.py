import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from gensim.models.word2vec import Word2Vec
from sklearn.cluster import KMeans as km

from data_transformation.db_env import DbEnv, db


class NLPClustering:
    def restrict_w2v(self, w2v, restricted_word_set):
        new_vectors = []
        new_vocab = {}
        new_index_to_key = []

        for i in range(len(w2v.index_to_key)):
            word = w2v.index_to_key[i]
            vec = w2v.vectors[i]
            if word in restricted_word_set:
                vocab_index = len(new_index_to_key)
                new_index_to_key.append(word)
                new_vocab[word] = vocab_index
                new_vectors.append(vec)

        w2v.key_to_index = new_vocab
        w2v.vectors = np.array(new_vectors)
        w2v.index_to_key = new_index_to_key

        return w2v

    def cluster_artist(self, model_res):
        data, n = model_res.vectors, 30
        km_model = km(n_clusters=n, algorithm='auto')
        km_model.fit(model_res.vectors)  # 학습
        predict_list = km_model.predict(data)
        df_nlp_cluster = pd.DataFrame([predict_list, model_res.index_to_key]).T

        return df_nlp_cluster


model = Word2Vec.load('../storage/word_dictionary/month6.model')
df_list = pd.read_pickle("../storage/df_raw_data/df_list_song_artist.pkl")

set_artist = set(df_list['artist'].values.tolist())
model_res = NLPClustering().restrict_w2v(model.wv, set_artist)

df_artist_cluster = NLPClustering().cluster_artist(model_res)

