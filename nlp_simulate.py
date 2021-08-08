import pandas as pd
from pymongo import MongoClient
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import LatentDirichletAllocation


client = MongoClient('localhost', 27017)

db1 = client.music_cow
col1 = db1.article_text


class ArticleNlp:
    def __init__(self):
        self.db_read()
        # self.tokenization()
        # self.topic_modeling

    def db_read(self):
        data = []
        article_list = col1.find({})
        df = pd.DataFrame(data)
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
            df['nlp_text'] = nlp_article
        df.to_pickle('df_article.pkl')

    def de_tokenization(self):
        detokenized_doc = []
        for i in range(len(text)):
            t = ' '.join(tokenized_doc[i])
            detokenized_doc.append(t)


ArticleNlp()

