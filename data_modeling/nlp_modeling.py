import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from gensim.models.word2vec import Word2Vec
from sklearn.cluster import KMeans as km
from pymongo import MongoClient
from gensim.models import KeyedVectors
import gensim
from gensim.models import CoherenceModel
import pyLDAvis.gensim_models
from gensim.corpora import Dictionary

from data_transformation.db_env import DbEnv, db
from data_crawling import artist_event_rule, artist_for_nlp


class NLPModeling:

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

    def counter_keyword(self, conn, cursor):
        self.list_artist = list(set(artist_event_rule.nnp_artist).union(set(artist_for_nlp.list_artist_NNP)))
        self.list_place = artist_event_rule.nnp_place
        self.list_event = artist_event_rule.nnp_event
        self.list_pro = artist_event_rule.nnp_pro

        self.artist_counter('2022-01-20')


    def artist_counter(self, date):
        sql = "SELECT token, tag, artist FROM newstokentemp WHERE date >= '%s'" % date
        cursor.execute(sql)
        conn.commit()
        list_tokens = cursor.fetchall()

        list_token_temp = list(map(lambda x: np.asarray(x[0].split(', ')), list_tokens))
        list_tag = list(map(lambda x: np.asarray(x[1].split(', ')), list_tokens))
        list_token = list(map(lambda x, y: np.stack((x, y), axis=1), list_token_temp, list_tag))
        list_artist = list(map(lambda x: x[2], list_tokens))

        array_token = list(map(lambda x: x[x[:,1] == 'NNP'][:,0], list_token))


        df_artist = self.sort_count_value(array_token, self.list_artist)
        df_place = self.sort_count_value(array_token, self.list_place)
        df_event = self.sort_count_value(array_token, self.list_event)
        df_pro = self.sort_count_value(array_token, self.list_pro)

        df_pro

    def filter_NNP(self, token):
        token[token[:,1] == 'NNP'][:,0]


    def sort_count_value(self, array_token, list_token):
        array_value = np.asarray([x for x in array_token if x in list_token])
        unique, counts = np.unique(array_value, return_counts=True)
        df_value = pd.DataFrame.from_dict([unique, counts]).T
        df_value = df_value.sort_values(by=[1], ascending=False)

        return df_value

    def import_token(self, conn, cursor, date):
        sql = "SELECT token, tag, artist FROM newstoken WHERE date >= '%s'" % date
        cursor.execute(sql)
        conn.commit()
        list_tokens = cursor.fetchall()

        list_token_temp = list(map(lambda x: np.asarray(x[0].split(', ')), list_tokens))
        list_tag = list(map(lambda x: np.asarray(x[1].split(', ')), list_tokens))
        list_token = list(map(lambda x, y: np.stack((x, y), axis=1), list_token_temp, list_tag))
        list_artist = list(map(lambda x: x[2], list_tokens))

        array_token = list(map(lambda x: x[x[:,1] == ('NNP' or 'NNG')][:,0].tolist(), list_token))

        return array_token

    def import_token_bert(self, conn, cursor, date, artist):
        try:
            sql = "SELECT token, tag, date, doc_num FROM newssentoken WHERE date >= '%s' and artist = '%s'" % (date, artist)
            cursor.execute(sql)
            conn.commit()
            tuple_sql = cursor.fetchall()
            array_token = np.array(tuple_sql)

            if len(array_token) > 80:
                list_token_temp = list(map(lambda x: np.asarray(x[0].split(', ')), tuple_sql))
                list_tag = list(map(lambda x: np.asarray(x[1].split(', ')), tuple_sql))
                list_token = list(map(lambda x, y: np.stack((x, y), axis=1), list_token_temp, list_tag))
                list_doc_num = list(map(lambda x: x[3], tuple_sql))

                list_tokens = list(map(lambda x: x[(x[:,1] == 'NNP') | (x[:,1] =='NNG')][:,0].tolist(), list_token))
                list_tokens = list(map(lambda x: ' '.join(x), list_tokens))


                list_time = array_token[:, 2]
                # list_time = ['%s/%s/%s' % (item.split('-')[2], item.split('-')[1], item.split('-')[0]) for item in list_time]

                return list_tokens, list_time, list_doc_num
            else:
                pass

        except:
            pass


# model = Word2Vec.load('../storage/word_dictionary/month6.model')
# df_list = pd.read_pickle("../storage/df_raw_data/df_list_song_artist.pkl")
#
# set_artist = set(df_list['artist'].values.tolist())
# model_res = NLPCount().restrict_w2v(model.wv, set_artist)
#
# df_artist_cluster = NLPCount().cluster_artist(model_res)

if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    db2 = client.article
    col5 = db2.article_info

    conn, cursor = DbEnv().connect_sql()

    # NewsArtistListCurrent = list(col5.find({}).distinct("artist"))

    # NLPModeling().counter_keyword(conn, cursor)
    # NLPModeling().w2v(conn, cursor, '2022-01-20')
    # [NLPModeling().topic_modeling(conn, cursor, '2022-01-20', artist) for artist in NewsArtistListCurrent]

    date = '2022-01-20'
    artist = '브레이브걸스'
    list_tokens, list_time = NLPModeling().import_token_bert(conn, cursor, date)

    # sql = "SELECT token, tag FROM newstokentemp WHERE date >= '%s' and artist='%s'" % (date, artist)
    # cursor.execute(sql)
    # conn.commit()
    # list_tokens = cursor.fetchall()

