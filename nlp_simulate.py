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
        self.tokenization()
        # self.after_token()
        # self.de_tokenization()
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
            sens = x['text'].split('.')
            nlp_article.append(re.sub(r'[^ ㄱ-ㅣ가-힣A-Za-z]', '', x['article_title']))
            for sen in sens:
                if sen == '':
                    continue
                nlp_article.append(re.sub(r'[^ ㄱ-ㅣ가-힣A-Za-z]', '', sen))
            df_temp = pd.DataFrame(nlp_article)
            df_nlp = df_nlp.append(df_temp)
        df_nlp.columns = ['text']
        df.to_pickle('df_article.pkl')
        df_nlp.to_pickle('df_sens_article.pkl')
        print(df_nlp)


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
                temp.append(j[0])
            tokenized_list.append(temp)
        print(tokenized_list)
        print('test')

    def de_tokenization(self):
        detokenized_doc = []
        for i in range(len(text)):
            t = ' '.join(tokenized_doc[i])
            detokenized_doc.append(t)

        text['headline_text'] = detokenized_doc # 다시 text['headline_text']에 재저장


ArticleNlp()

