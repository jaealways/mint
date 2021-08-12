import pandas as pd
from pymongo import MongoClient
import re
from kiwipiepy import Kiwi
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import LatentDirichletAllocation


client = MongoClient('localhost', 27017)

db1 = client.music_cow
col1 = db1.article_text


class ArticleNlp:
    def __init__(self):
        # self.db_read()
        # self.tokenization()
        self.after_token()
        # self.topic_modeling

    def db_read(self):
        data = []
        article_list = col1.find({})
        df = pd.DataFrame(data)
        df_nlp = pd.DataFrame(data)
        for x in article_list:
            nlp_article = []
            result = {'num': x['num'], 'song_title': x['song_title'], 'song_artist': x['song_artist'], 'link': x['link'],
                      'article_title': x['article_title'], 'publish': x['publish'], 'text': x['text'], 'date': x['date']}
            df = df.append(result, ignore_index=True)
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
        df_nlp = pd.read_pickle('df_sens_article_after.pkl')
        tokenized_list = []
        for i in df_nlp['tokenized']:
            temp = []
            for j in i[0][0]:
                if j[0] in ['co', 'kr', 'com']:
                    continue
                if j[1] in ['NNG', 'NNP', 'NNB', 'NR', 'NP', 'VV', 'VA', 'VX', 'XR', 'SL']:
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
        vectorizer = TfidfVectorizer(max_features=1000, min_df=8)
        X = vectorizer.fit_transform(df_nlp['tokenized'])
        X.shape

        lda_model = LatentDirichletAllocation(n_components=5, learning_method='online', random_state=777, max_iter=1)
        lda_top = lda_model.fit_transform(X)
        terms = vectorizer.get_feature_names()

        for idx, topic in enumerate(lda_model.components_):
            print("Topic %d:" % (idx+1), [(terms[i], topic[i].round(2)) for i in topic.argsort()[:-15 - 1:-1]])

        lda_top = pd.DataFrame(lda_top)
        df_by_topic = pd.concat([df_nlp, lda_top], axis=1)
        df_by_topic.to_pickle('df_by_topic.pkl')


ArticleNlp()

