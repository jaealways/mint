from pymongo import MongoClient
import pandas as pd
import itertools
from konlpy.tag import Mecab
import numpy as np
from collections import Counter
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences

class NLPKeyword:
    def __init__(self):
        client = MongoClient('localhost', 27017)
        db = client.article
        self.col = db.article_info

    def db_to_article(self, str_date, end_date):
        df_artist_nlp = pd.read_pickle("../storage/df_raw_data/df_artist_nlp.pkl")
        articles1 = list(self.col.find({'$and': [{'text': {'$exists': True}}, {'date': {'$gte': str_date, '$lte': end_date}}]}))
        title1 = list(map(lambda x: x['article_title'], articles1))
        text1 = list(map(lambda x: x['text'], articles1))
        artist1 = list(map(lambda x: x['artist'], articles1))
        artist2 = list(map(lambda x: df_artist_nlp[df_artist_nlp['nlp_query'] == x]['nlp_dict'].values[0], artist1))

        date1 = list(map(lambda x: x['date'], articles1))
        df_article = pd.DataFrame(list(zip(title1, text1, artist2, date1)), columns=['title', 'text1', 'artist', 'date'])
        df_article.to_pickle("../storage/df_raw_data/df_article_%s_%s.pkl" % (str_date, end_date))
        print("df_article")

        return df_article

    def article_to_sen(self, df_article, str_date, end_date):
        df_article = df_article[(df_article['date'] >= str_date) & (df_article['date'] <= end_date)]
        df_sen = pd.DataFrame([])
        df_article['text2'] = list(map(lambda x: x.split('. '), df_article['text1']))

        for x in df_article.iterrows():
            sen_list = [y for y in x[-1]['text2']]
            sen_list.append(x[-1]['title'])
            sen_df_artist, sen_df_date = x[-1]['artist'], x[-1]['date']
            df_temp = pd.DataFrame(sen_list, columns=['text'])
            df_temp['artist'], df_temp['date'] = sen_df_artist, sen_df_date

            df_sen = pd.concat([df_sen, df_temp])

        pre_sentences = list(map(lambda x: x.replace("[^A-za-z가-힣ㄱ-ㅎㅏㅡㅣ ]", "").strip(), df_sen['text']))
        pre_sentences = list(map(lambda x: x.replace("""[|]|"|'|\n|\t""", ''), pre_sentences))

        df_sen['text'] = pre_sentences
        df_sen.to_pickle("../storage/df_raw_data/df_sen_%s_%s.pkl" % (str_date, end_date))

        print("df_sen")

        return df_sen

    def sen_to_token(self, df_sen, str_date, end_date, artist='all'):
        df_token = pd.DataFrame([])

        if artist == 'all':
            df_sen = df_sen[(df_sen['date'] >= str_date) & (df_sen['date'] <= end_date)]
        else:
            df_sen = df_sen[(df_sen['artist'] == artist) & (df_sen['date'] >= str_date) & (df_sen['date'] <= end_date)]

        mecab = Mecab(dicpath=r'C:\mecab\mecab-ko-dic')
        df_token['token'] = df_sen.text.apply(lambda x: mecab.pos(x))

        df_NNP_temp = df_token['token'].apply(lambda x: [y[0] for y in x if y[1]=="NNP"])
        df_NNG_temp = df_token['token'].apply(lambda x: [y[0] for y in x if y[1]=="NNG"])

        list_NNP, list_NNG = [], []
        [list_NNP.extend(x) for x in df_NNP_temp.to_list()]
        [list_NNG.extend(x) for x in df_NNG_temp.to_list()]

        count_NNP, count_NNG = Counter(list_NNP), Counter(list_NNG)
        df_NNP = pd.DataFrame.from_dict(count_NNP, orient='index').reset_index()
        df_NNG = pd.DataFrame.from_dict(count_NNG, orient='index').reset_index()

        return df_NNP, df_NNG

    # def token_to_tag(self, df_token):
    #     mecab = Mecab(dicpath=r'C:\mecab\mecab-ko-dic')
    #     df_sen['pre_text'] = df_token.text.apply(lambda x: mecab.nouns(x))
    #     # tokenizer = Tokenizer()
    #     # tokenizer.fit_on_texts(df_sen['pre_text'])
    #     #
    #     # len(tokenizer.word_index)**0.25

str_date, end_date = '2020-12-20', '2022-01-20'
df_article = NLPKeyword().db_to_article(str_date, end_date)
df_article = pd.read_pickle("../storage/df_raw_data/df_article_%s_%s.pkl" % (str_date, end_date))

df_sen = NLPKeyword().article_to_sen(df_article, str_date, end_date)
df_sen = pd.read_pickle("../storage/df_raw_data/df_sen_%s_%s.pkl" % (str_date, end_date))

# artist = '별'
# df_NNP, df_NNG = NLPKeyword().sen_to_token(df_sen, str_date, end_date, artist)
