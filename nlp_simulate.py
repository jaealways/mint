import pandas as pd
from pymongo import MongoClient
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import LatentDirichletAllocation


client = MongoClient('localhost', 27017)

db1 = client.music_cow
col1 = db1.daily_article


class ArticleNlp:
    def __init__(self):
        self.db_read()
        self.tokenization()
        # self.topic_modeling

    def db_read(self):
        data = []
        article_list = col1.find({})
        df = pd.DataFrame(data)
        for x in article_list:
            result = {'num': x['num'], 'song_title': x['song_title'], 'song_artist': x['song_artist'], 'link': x['link'],
                      'article_title': x['article_title'], 'publish': x['publish'], 'text': x['text'], 'date': x['date']}
            df = df.append(result, ignore_index=True)
        df.to_pickle('df_article.pkl')

    def tokenization(self):
        df = pd.read_pickle('df_article.pkl')
        print(df)
        text = df['text']


ArticleNlp()

