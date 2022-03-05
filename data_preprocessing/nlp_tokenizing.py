from pymongo import MongoClient
import pandas as pd
import itertools
from konlpy.tag import Mecab
import numpy as np
from collections import Counter
import csv

from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences


class NLPTokenize:
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
        # 아티스트 내림차순으로 df 정렬
        # counter로 list 속에 있는 문장 갯수 새서 별도의 dict로 저장
        # 추후에 loop 돌면서 저장된 갯수만큼 df 생성

        df_article = df_article[(df_article['date'] >= str_date) & (df_article['date'] <= end_date)]
        df_article = df_article.sort_values(["artist", "date"])

        df_article['text2'] = list(map(lambda x: x.split('. '), df_article['text1']))
        list(map(lambda x, y: x.append(y), df_article['text2'], df_article['title']))

        df_article['len_text'] = list(map(lambda x: len(x), df_article['text2']))

        list_sen, list_artist, list_date = [], [], []

        list(map(lambda x: list_sen.extend(x), df_article['text2']))
        list(map(lambda x, y: list_artist.extend([x for z in range(y)]), df_article['artist'], df_article['len_text']))
        list(map(lambda x, y: list_date.extend([x for z in range(y)]), df_article['date'], df_article['len_text']))

        df_sen = pd.DataFrame([list_sen, list_artist, list_date], index=['text', 'artist', 'date']).T
        pre_sentences = list(map(lambda x: x.replace("[^A-za-z가-힣ㄱ-ㅎㅏㅡㅣ ]", "").strip(), df_sen['text']))
        pre_sentences = list(map(lambda x: x.replace("""[|]|\n|\t""", ''), pre_sentences))

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

        df_NNP_temp = df_token['token'].apply(lambda x: [y[0] for y in x if y[1] == "NNP"])
        df_NNG_temp = df_token['token'].apply(lambda x: [y[0] for y in x if y[1] == "NNG"])

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


str_date, end_date = '2021-12-20', '2021-12-21'
# # df_article = NLPTokenize().db_to_article(str_date, end_date)
# df_article = pd.read_pickle("../storage/df_raw_data/df_article_%s_%s.pkl" % (str_date, end_date))
# #
# # df_sen = NLPTokenize().article_to_sen(df_article, str_date, end_date)
# df_sen = pd.read_pickle("../storage/df_raw_data/df_sen_%s_%s.pkl" % (str_date, end_date))
# #
artist = '브레이브걸스'
df_sen = pd.read_pickle("../storage/df_raw_data/df_sen_%s_%s.pkl" % ("2021-12-20", "2021-12-21"))

df_NNP, df_NNG = NLPTokenize().sen_to_token(df_sen, str_date, end_date, artist)

# NLPTokenize().update_mecab_dict()

