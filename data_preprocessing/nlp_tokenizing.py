from pymongo import MongoClient
import pandas as pd
import itertools
from konlpy.tag import Mecab
import numpy as np
from collections import Counter
import csv
from data_transformation.db_env import DbEnv, db
import re


from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences


class NLPTokenize:
    def __init__(self):
        client = MongoClient('localhost', 27017)
        db = client.article
        self.col = db.article_info

    def db_to_article(self):
        df_artist_nlp = pd.read_pickle("../storage/df_raw_data/df_artist_nlp.pkl")

        articles1 = list(self.col.find({'date_crawler': '2022-03-16'}))
        title1 = list(map(lambda x: x['article_title'], articles1))
        text1 = list(map(lambda x: x['text'], articles1))
        artist1 = list(map(lambda x: x['artist'], articles1))

        # list_temp = set(artist1) - set(df_artist_nlp['nlp_query'].values)

        artist2 = list(map(lambda x: df_artist_nlp[df_artist_nlp['nlp_query'] == x]['nlp_dict'].values[0], artist1))

        date1 = list(map(lambda x: x['date'], articles1))
        list_article = list(zip(title1, text1, artist2, date1))

        # df_article = pd.DataFrame(list(zip(title1, text1, artist2, date1)), columns=['title', 'text1', 'artist', 'date'])
        # df_article.to_pickle("../storage/df_raw_data/df_article_%s_%s.pkl" % (str_date, end_date))
        print("df_article")

        return list_article

    def article_to_sen(self, list_article, conn, cursor):
        article_split = list(map(lambda x: x[1].split('. '), list_article))
        list(map(lambda x, y: x.append(y[0]), article_split, list_article))
        len_article = list(map(lambda x: len(x), article_split))

        list_sen, list_artist, list_date = [], [], []

        list(map(lambda x: list_sen.extend(x), article_split))
        list(map(lambda x, y: list_artist.extend([x[2] for z in range(y)]), list_article, len_article))
        list(map(lambda x, y: list_date.extend([x[3] for z in range(y)]), list_article, len_article))

        pre_sentences = list(map(lambda x: x.replace("[^A-za-z가-힣ㄱ-ㅎㅏㅡㅣ ]", "").strip(), list_sen))
        pre_sentences = list(map(lambda x: x.replace("[|]|\n|\t|\\", ""), pre_sentences))
        pre_sentences = list(map(lambda x: re.compile("""["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+""", flags=re.UNICODE).sub(r'', x), pre_sentences))
        list_sen = pre_sentences
        list_date_crawler = ['2022-03-16' for x in range(len(list_sen))]

        list_sens = list(zip(list_sen, list_artist, list_date, list_date_crawler))
        # df_sen.to_pickle("../storage/df_raw_data/df_sen_%s_%s.pkl" % (str_date, end_date))

        sql = "INSERT INTO newssen VALUES (%s, %s, %s, %s)"
        cursor.executemany(sql, list_sens)
        conn.commit()

        print("df_sen")


    def sen_to_token(self, df_sen, cursor, conn):
        df_token = pd.DataFrame([])

        if artist == 'all':
            df_sen = df_sen[(df_sen['date'] >= str_date) & (df_sen['date'] <= end_date)]
        else:
            df_sen = df_sen[(df_sen['artist'] == artist) & (df_sen['date'] >= str_date) & (df_sen['date'] <= end_date)]

        mecab = Mecab(dicpath=r'C:\mecab\mecab-ko-dic')
        df_token['token'] = df_sen.text.apply(lambda x: mecab.pos(x))
        df_token['artist'], df_token['date'] = df_sen['artist'], df_sen['date']

        print('sen_to_token')

        return df_token

    def token_to_tag(self, df_token):
        df_NNP_temp = df_token['token'].apply(lambda x: [y[0] for y in x if y[1]=="NNP"])
        df_NNG_temp = df_token['token'].apply(lambda x: [y[0] for y in x if y[1]=="NNG"])

        list_NNP, list_NNG = [], []
        [list_NNP.extend(x) for x in df_NNP_temp.to_list()]
        [list_NNG.extend(x) for x in df_NNG_temp.to_list()]

        count_NNP, count_NNG = Counter(list_NNP), Counter(list_NNG)
        df_NNP = pd.DataFrame.from_dict(count_NNP, orient='index').reset_index()
        df_NNG = pd.DataFrame.from_dict(count_NNG, orient='index').reset_index()

        df_NNP.to_pickle("../storage/df_raw_data/df_NNP_%s_%s.pkl" % (str_date, end_date))
        df_NNG.to_pickle("../storage/df_raw_data/df_NNG_%s_%s.pkl" % (str_date, end_date))

        return df_NNP, df_NNG

    # def token_to_tag(self, df_token):
    #     mecab = Mecab(dicpath=r'C:\mecab\mecab-ko-dic')
    #     df_sen['pre_text'] = df_token.text.apply(lambda x: mecab.nouns(x))
    #     # tokenizer = Tokenizer()
    #     # tokenizer.fit_on_texts(df_sen['pre_text'])
    #     #
    #     # len(tokenizer.word_index)**0.25

    def update_mecab_dict(self):
        client = MongoClient('localhost', 27017)
        db = client.music_cow
        col = db.musicCowDataTemp

        dict_title = col.find({}, {'song_title': {'$slice': [1, 1]}})
        list_title = list(map(lambda x: x['song_title'], dict_title))
        f = open('C:/mecab/user-dic/nnp.csv', 'w', newline='', encoding='utf8')

        for i in list_title:
            print(i)
            text = '{0},,,,NNP,*,F,{0},*,*,*,*,*'.format(i)
            wr = csv.writer(f, delimiter=' ', escapechar=' ', quoting=csv.QUOTE_NONE)
            wr.writerow([text])

        f.close()

        f = open('C:/mecab/user-dic/nnp.csv', 'r', newline='', encoding='utf8')

        rdr = csv.reader(f)
        lines = []
        for line in rdr:
            line[0] = line[0].replace('  ', ' ')
            line[7] = line[7].replace('  ', ' ')
            print(line)
            lines.append(line)

        f = open('C:/mecab/user-dic/nnp.csv', 'w', newline='', encoding='utf8')
        wr = csv.writer(f)
        wr.writerows(lines)

        dict_artist = col.find({}, {'song_artist': {'$slice': [1, 1]}})
        list_artist = list(map(lambda x: x['song_artist'], dict_artist))
        list_artist_temp = set(list_artist)

        df_artist_nlp = pd.read_pickle("../storage/df_raw_data/df_artist_nlp.pkl")

        list_df_artist_nlp = df_artist_nlp['music_cow'].to_list()

        list_diff = list(set(list_artist_temp) - set(list_df_artist_nlp))

        if len(list_diff) != 0:
            print(list_diff)
            quit()
        else:
            pass

        list_artist = list(map(lambda x: df_artist_nlp[df_artist_nlp['music_cow'] == x]['nlp_dict'].values[0], list_artist_temp))

        f = open('C:/mecab/user-dic/person.csv', 'w', newline='', encoding='utf8')
        for i in list_artist:
            print(i)
            text = '{0},,,,NNP,*,F,{0},*,*,*,*,*'.format(i)
            wr = csv.writer(f, delimiter=' ', escapechar=' ', quoting=csv.QUOTE_NONE)
            wr.writerow([text])


conn, cursor = DbEnv().connect_sql()

str_date, end_date = '2018-12-20', '2023-03-15'
list_article = NLPTokenize().db_to_article()
# df_article = pd.read_pickle("../storage/df_raw_data/df_article_%s_%s.pkl" % (str_date, end_date))

df_sen = NLPTokenize().article_to_sen(list_article, conn, cursor)
# df_sen = pd.read_pickle("../storage/df_raw_data/df_sen_%s_%s.pkl" % (str_date, end_date))

# artist = 'all'
# str_date, end_date = '2021-12-13', '2022-03-15'
#
# df_token = NLPTokenize().sen_to_token(df_sen, str_date, end_date, artist)
# df_NNP, df_NNG = NLPTokenize().token_to_tag(df_token)
# # NLPTokenize().update_mecab_dict()

