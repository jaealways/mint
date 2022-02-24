from pymongo import MongoClient
import pandas as pd
import itertools
from konlpy.tag import Mecab
import numpy as np
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences

class NLPKeyword:
    def __init__(self):
        client = MongoClient('localhost', 27017)
        db = client.article
        self.col = db.article_info

    def article_merge(self, str_date, end_date):
        df_artist_nlp = pd.read_pickle("../storage/df_raw_data/df_artist_nlp.pkl")
        articles1 = list(self.col.find({'$and': [{'text': {'$exists': True}}, {'date': {'$gte': str_date, '$lte': end_date}}]}))
        title1 = list(map(lambda x: x['article_title'], articles1))
        text1 = list(map(lambda x: x['text'], articles1))
        artist1 = list(map(lambda x: x['artist'], articles1))
        artist2 = []
        for x in artist1:
            try:
                artist_temp = df_artist_nlp[df_artist_nlp['nlp_query'] == x]['nlp_dict'].values[0]
            except:
                artist_temp = np.nan
            artist2.append(artist_temp)
        date1 = list(map(lambda x: x['date'], articles1))
        tmp_list = list(map(lambda x: x.split('. '), text1))

        temp_sen_df = pd.DataFrame(list(zip(title1, tmp_list, artist2, date1)), columns=['title', 'text1', 'artist', 'date'])

        sen_df = pd.DataFrame([], columns=['text', 'artist', 'date'])

        for x in temp_sen_df.iterrows():
            sen_list = [y for y in x[-1]['text1']]
            sen_list.append(x[-1]['title'])
            sen_df_artist, sen_df_date = x[-1]['artist'], x[-1]['date']

            for z in sen_list:
                sen_df = sen_df.append([z, sen_df_artist, sen_df_date])

        pre_sentences = list(map(lambda x: x.replace("[^A-za-z가-힣ㄱ-ㅎㅏㅡㅣ ]", "").strip(), sen_df['text']))
        pre_sentences = list(map(lambda x: x.replace('[', ''), pre_sentences))
        pre_sentences = list(map(lambda x: x.replace(']', ''), pre_sentences))
        pre_sentences = list(map(lambda x: x.replace('"', ''), pre_sentences))

        sen_df['text'] = pre_sentences
        sen_df.to_pickle("../storage/df_raw_data/df_sen.pkl")

        return sen_df

    def article_tokenize(self, df_sen):
        mecab = Mecab(dicpath=r'C:\mecab\mecab-ko-dic')
        df_temp = df_sen.text.apply(lambda x: mecab.pos(x))
        df_temp.to_pickle("../storage/df_raw_data/df_temp.pkl")

        return df_temp

    # def token_

        # df_sen['pre_text'] = df_sen.text.apply(lambda x: mecab.nouns(x))
        # tokenizer = Tokenizer()
        # tokenizer.fit_on_texts(df_sen['pre_text'])
        #
        # len(tokenizer.word_index)**0.25


df_sen = NLPKeyword().article_merge('2021-11-21', '2021-12-21')
# df_sen = pd.read_pickle("../storage/df_raw_data/df_sen.pkl")
df_temp = NLPKeyword().article_tokenize(df_sen)
# df_nnp =

