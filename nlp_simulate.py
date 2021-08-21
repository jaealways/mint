import pandas as pd
from pymongo import MongoClient
import re
from kiwipiepy import Kiwi
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import LatentDirichletAllocation, PCA
from sklearn.cluster import KMeans


client = MongoClient('localhost', 27017)

db1 = client.music_cow
col1 = db1.article_text
col2 = db1.music_list_split


class ArticleNlp:
    def __init__(self):
        # self.db_read()
        # self.tokenization()
        # self.after_token()
        # self.for_read_df()
        # self.test_topic3()
        self.document_clustering()

    def db_read(self):
        data = []
        article_list = col1.find({})
        df = pd.DataFrame(data)
        df_nlp = pd.DataFrame(data)
        df_artist = []
        for x in article_list:
            nlp_article = []
            result = {'num': x['num'], 'song_title': x['song_title'], 'song_artist': x['song_artist'], 'link': x['link'],
                      'article_title': x['article_title'], 'publish': x['publish'], 'text': x['text'], 'date': x['date']}
            df = df.append(result, ignore_index=True)
            df_artist.append(x['song_artist'])
            sen = x['text'].replace('.', '')
            nlp_article.append(re.sub(r'[^ ㄱ-ㅣ가-힣A-Za-z]', '', x['article_title']) + re.sub(r'[^ ㄱ-ㅣ가-힣A-Za-z]', '', sen))
            df_temp = pd.DataFrame(nlp_article)
            df_nlp = df_nlp.append(df_temp)
        df_nlp.columns = ['text']
        df.to_pickle('df_article.pkl')
        df_nlp.to_pickle('df_sens_article.pkl')

    def tokenization(self):
        df = pd.read_pickle('df_article.pkl')
        df_nlp = pd.read_pickle('df_sens_article.pkl')

        kiwi = Kiwi()
        kiwi.prepare()
        df_nlp['tokenized'] = df_nlp['text'].apply(lambda x: kiwi.analyze(x))
        df_nlp.to_pickle('df_sens_article_after.pkl')

    def after_token(self):
        db_artist_list = col2.find({})
        artist_list = []
        for y in db_artist_list:
            if 'song_artist_main_kor1' in y['list_split']:
                artist_list.append(y['list_split']['song_artist_main_kor1'])
            else:
                artist_list.append(y['list_split']['song_artist_main_eng1'])

        df_nlp = pd.read_pickle('df_sens_article_after.pkl')
        tokenized_list = []
        for i in df_nlp['tokenized']:
            temp = []
            for j in i[0][0]:
                if j[0] in artist_list:
                    continue
                if j[0] in ['co', 'kr', 'com', 'outlet', '아웃렛', '영기', '안성훈', '박성', 'msg워너비', 'msg워너', '유재석',
                            '김신영', '임영웅', '이찬원', '김희재', '세븐틴', '장민호', '방탄소년단']:
                    continue
                if j[1] in ['NNG', 'NNP', 'NNB', 'NR', 'NP', 'MM', 'MAG', 'VV', 'VA', 'VX', 'XR', 'SL']:
                    temp.append(j[0])
            tokenized_list.append(temp)
        df_nlp['after_token'] = tokenized_list
        index_new = [a for a in range(len(df_nlp))]
        df_nlp = df_nlp.set_index([index_new])
        tokenized_list = df_nlp['after_token'].apply(lambda x: [word for word in x if len(word) > 1])
        index_new = [a for a in range(len(df_nlp['tokenized']))]
        tokenized_list.index = index_new
        print(tokenized_list[:5])

        detokenized_doc = []
        for i in range(len(tokenized_list)):
            t = ' '.join(tokenized_list[i])
            detokenized_doc.append(t)

        df_nlp['tokenized'] = detokenized_doc
        vectorizer = TfidfVectorizer(max_features=1000, min_df=15)
        X = vectorizer.fit_transform(df_nlp['tokenized'])
        X.shape

        lda_model = LatentDirichletAllocation(n_components=4, learning_method='online', random_state=777, max_iter=1)
        lda_top = lda_model.fit_transform(X)
        terms = vectorizer.get_feature_names()

        for idx, topic in enumerate(lda_model.components_):
            print("Topic %d:" % (idx+1), [(terms[i], topic[i].round(2)) for i in topic.argsort()[:-15 - 1:-1]])

        lda_top = pd.DataFrame(lda_top)
        df_by_topic = pd.concat([df_nlp, lda_top], axis=1)
        df_by_topic.to_pickle('df_by_topic_title.pkl')

    def for_read_df(self):
        df_by_topic = pd.read_pickle('df_by_topic_title.pkl')
        list_id = []
        for h_num in range(len(df_by_topic)):
            df_id = pd.to_numeric(df_by_topic.iloc[h_num][3:7]).idxmax()
            if df_id == 2:
                list_id.append(h_num)
        db_artist_list = col2.find({})
        artist_list = []
        for y in db_artist_list:
            if 'song_artist_main_kor1' in y['list_split']:
                artist_list.append(y['list_split']['song_artist_main_kor1'])
            else:
                artist_list.append(y['list_split']['song_artist_main_eng1'])

        df_nlp = pd.read_pickle('df_sens_article_after.pkl')
        index_new = [a for a in range(len(df_nlp))]
        df_nlp = df_nlp.set_index([index_new])
        df_nlp = df_nlp.loc[list_id, :]
        tokenized_list = []
        for i in df_nlp['tokenized']:
            temp = []
            for j in i[0][0]:
                if j[0] in artist_list:
                    continue
                if j[0] in ['co', 'kr', 'com', 'outlet', '아웃렛', '영기', '안성훈', '박성', 'msg워너비', 'msg워너', '유재석',
                            '김신영', '임영웅', '이찬원', '김희재', '세븐틴', '장민호', '방탄소년단']:
                    continue
                if j[1] in ['NNG', 'NNP', 'NNB', 'NR', 'NP', 'MM', 'MAG', 'VV', 'VA', 'VX', 'XR', 'SL']:
                    temp.append(j[0])
            tokenized_list.append(temp)
        df_nlp['after_token'] = tokenized_list
        index_new = [a for a in range(len(df_nlp))]
        df_nlp = df_nlp.set_index([index_new])
        tokenized_list = df_nlp['after_token'].apply(lambda x: [word for word in x if len(word) > 1])
        index_new = [a for a in range(len(df_nlp['tokenized']))]
        tokenized_list.index = index_new
        print(tokenized_list[:5])

        detokenized_doc = []
        for i in range(len(tokenized_list)):
            t = ' '.join(tokenized_list[i])
            detokenized_doc.append(t)

        df_nlp['tokenized'] = detokenized_doc
        vectorizer = TfidfVectorizer(max_features=1000, min_df=15)
        X = vectorizer.fit_transform(df_nlp['tokenized'])
        X.shape

        lda_model = LatentDirichletAllocation(n_components=2, learning_method='online', random_state=777, max_iter=1)
        lda_top = lda_model.fit_transform(X)
        terms = vectorizer.get_feature_names()

        for idx, topic in enumerate(lda_model.components_):
            print("Topic %d:" % (idx+1), [(terms[i], topic[i].round(2)) for i in topic.argsort()[:-15 - 1:-1]])

        lda_top = pd.DataFrame(lda_top)
        df_by_topic = pd.concat([df_nlp, lda_top], axis=1)
        df_by_topic.to_pickle('df_by_topic_3.pkl')

    def test_topic3(self):
        df_by_topic = pd.read_pickle('df_by_topic_title.pkl')
        list_id = []
        for h_num in range(len(df_by_topic)):
            df_id = pd.to_numeric(df_by_topic.iloc[h_num][3:7]).idxmax()
            list_id.append(df_id)
        print(list_id.count(0), list_id.count(1), list_id.count(2), list_id.count(3))

    def document_clustering(self):
        db_artist_list = col2.find({})
        artist_list = []
        for y in db_artist_list:
            if 'song_artist_main_kor1' in y['list_split']:
                artist_list.append(y['list_split']['song_artist_main_kor1'])
            else:
                artist_list.append(y['list_split']['song_artist_main_eng1'])

        df_nlp = pd.read_pickle('df_sens_article_after.pkl')
        tokenized_list = []
        for i in df_nlp['tokenized']:
            temp = []
            for j in i[0][0]:
                if j[0] in artist_list:
                    continue
                if j[0] in ['co', 'kr', 'com', 'outlet', '아웃렛', '영기', '안성훈', '박성', 'msg워너비', 'msg워너', '유재석',
                            '김신영', '임영웅', '이찬원', '김희재', '세븐틴', '장민호', '방탄소년단']:
                    continue
                if j[1] in ['NNG', 'NNP', 'NNB', 'NR', 'NP', 'MM', 'MAG', 'VV', 'VA', 'VX', 'XR', 'SL']:
                    temp.append(j[0])
            tokenized_list.append(temp)
        df_nlp['after_token'] = tokenized_list
        index_new = [a for a in range(len(df_nlp))]
        df_nlp = df_nlp.set_index([index_new])
        tokenized_list = df_nlp['after_token'].apply(lambda x: [word for word in x if len(word) > 1])
        index_new = [a for a in range(len(df_nlp['tokenized']))]
        tokenized_list.index = index_new
        print(tokenized_list[:5])

        detokenized_doc = []
        for i in range(len(tokenized_list)):
            t = ' '.join(tokenized_list[i])
            detokenized_doc.append(t)

        df_nlp['tokenized'] = detokenized_doc
        vectorizer = TfidfVectorizer(max_features=1000, min_df=15)
        X = vectorizer.fit_transform(df_nlp['tokenized'])

        km_cluster = KMeans(n_clusters=5, max_iter=10000, random_state=0)
        km_cluster.fit(X)
        cluster_label = km_cluster.labels_
        cluster_centers = km_cluster.cluster_centers_

        df_nlp['cluster_label'] = cluster_label
        df_nlp.head()

        df_nlp[df_nlp['cluster_label'] == 0]
        df_nlp[df_nlp['cluster_label'] == 1]
        df_nlp[df_nlp['cluster_label'] == 2]
        df_nlp[df_nlp['cluster_label'] == 3]
        df_nlp[df_nlp['cluster_label'] == 4]

        df_nlp.to_pickle('df_document_clustering.pkl')

    def main_word(self, cluster_model, cluster_data, feature_names, clusters_num, top_n_features=10):
        cluster_details = {}
        centroid_feature_ordered_ind = cluster_model.cluster_centers_.argsort()[:, ::-1]

        for cluster_num in range(clusters_num):
            cluster_details[cluster_num] = {}
            cluster_details[cluster_num]['cluster'] = cluster_num

            top_feature_indexes = centroid_feature_ordered_ind[cluster_num, :top_n_features]
            top_features = [feature_names[ind] for ind in top_feature_indexes]

            top_feature_values = cluster_model.cluster_centers_[cluster_num, top_feature_indexes].tolist()

            cluster_details[cluster_num]['top_features'] = top_features
            cluster_details[cluster_num]['top_features_value'] = top_feature_values
            filenames = cluster_data[cluster_data['cluster_label'] == cluster_num]['filename']
            filenames = filenames.values.tolist()

            cluster_details[cluster_num]['filenames'] = filenames

        return cluster_details



ArticleNlp()

