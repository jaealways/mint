from pymongo import MongoClient
import pandas as pd
import itertools
from konlpy.tag import Mecab
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences

class NLPKeyword:
    def __init__(self):
        client = MongoClient('localhost', 27017)
        db = client.article
        self.col = db.article_info

    def article_merge(self):
        articles1 = self.col.find({'$and': [{'text': {'$exists': True}},
                                       {'$or': [{'date': {'$regex': '2022-01'}}, {'date': {'$regex': '2021-12'}},
                                                {'date': {'$regex': '2021-11'}}, {'date': {'$regex': '2021-10'}},
                                                {'date': {'$regex': '2021-09'}}, {'date': {'$regex': '2021-08'}},
                                                {'date': {'$regex': '2021-07'}}, {'date': {'$regex': '2021-06'}}]}]})
        title1 = list(map(lambda x: x['article_title'], articles1))
        articles1 = self.col.find({'$and': [{'text': {'$exists': True}},
                                       {'$or': [{'date': {'$regex': '2022-01'}}, {'date': {'$regex': '2021-12'}},
                                                {'date': {'$regex': '2021-11'}}, {'date': {'$regex': '2021-10'}},
                                                {'date': {'$regex': '2021-09'}}, {'date': {'$regex': '2021-08'}},
                                                {'date': {'$regex': '2021-07'}}, {'date': {'$regex': '2021-06'}}]}]})
        text1 = list(map(lambda x: x['text'], articles1))

        tmp_list = list(map(lambda x: x.split('. '), text1))
        texts1 = list(itertools.chain(*tmp_list))

        sentences = title1 + texts1

        pre_sentences = list(map(lambda x: x.replace("[^A-za-z가-힣ㄱ-ㅎㅏㅡㅣ ]", "").strip(), sentences))
        pre_sentences = list(map(lambda x: x.replace('[', ''), pre_sentences))
        pre_sentences = list(map(lambda x: x.replace(']', ''), pre_sentences))
        pre_sentences = list(map(lambda x: x.replace('"', ''), pre_sentences))

        sen_df = pd.DataFrame(pre_sentences, columns=['text'])
        sen_df.to_pickle("../storage/df_raw_data/df_sen.pkl")

        return sen_df

    def article_tokenize(self, df_sen):
        df_temp = pd.DataFrame()
        mecab = Mecab(dicpath=r'C:\mecab\mecab-ko-dic')
        df_temp = df_sen.text.apply(lambda x: mecab.pos(x))
        df_temp.to_pickle("../storage/df_raw_data/df_temp.pkl")

        # df_sen['pre_text'] = df_sen.text.apply(lambda x: mecab.nouns(x))
        # tokenizer = Tokenizer()
        # tokenizer.fit_on_texts(df_sen['pre_text'])
        #
        # len(tokenizer.word_index)**0.25


df_sen = pd.read_pickle("../storage/df_raw_data/df_sen.pkl")
NLPKeyword().article_tokenize(df_sen)

