import pandas as pd
from tqdm import tqdm
from bertopic import BERTopic
from datetime import datetime
from pymongo import MongoClient
from sklearn.feature_extraction.text import CountVectorizer
from dateutil.relativedelta import relativedelta

from data_modeling.nlp_modeling import NLPModeling
from data_transformation.db_env import DbEnv
from data_crawling.artist_for_nlp import df_nlp


def bertopic(artist, date):
    dict_news = {}

    artist_mc = df_artist_nlp[df_artist_nlp['nlp_dict'] == artist]['music_cow'].values[0]
    artist_query = df_artist_nlp[df_artist_nlp['nlp_dict'] == artist]['nlp_query'].values[0]

    conn, cursor = DbEnv().connect_sql()
    list_tokens, list_time, list_doc_num = NLPModeling().import_token_bert(conn, cursor, date, artist_mc)

    # try:
    if len(list_tokens) > 100:
        topic_num = round(len(list_tokens) ** 0.25)
        print('%s, %s, 주제 %s개, 기사 %s개' % (artist, date, topic_num, len(list_tokens)))
        vectorizer = CountVectorizer(input=list_tokens, max_features=3000)
        model = BERTopic(embedding_model="sentence-transformers/xlm-r-100langs-bert-base-nli-stsb-mean-tokens", \
                         vectorizer_model=vectorizer, nr_topics=topic_num, top_n_words=20, calculate_probabilities=True)

        topics, _ = model.fit_transform(list_tokens)
        repre_docs = model.representative_docs
        dict_news['artist'] = artist
        dict_news['song_num'] = col1.find({'song_artist': artist_mc}).distinct('num')

        for k, v in repre_docs.items():
            news_text = max(v, key=len)
            index_text = list_tokens.index(news_text)
            doc_num_text = list_doc_num[index_text]
            date_text = list_time[index_text]
            link_text = col5.find({'artist': artist_query, 'date': date_text, 'doc_num': int(doc_num_text)}).distinct('link')
            dict_news[k] = link_text[0]

        df_time = model.topics_over_time(list_tokens, topics, list_time, nr_bins=20)
        fig = model.visualize_topics_over_time(df_time, top_n_topics=10)

        fig.write_html("%s.html" % artist)

        return dict_news


    else:
        print('%s, %s, 기사 %s개로 패스' % (artist, date, len(list_tokens)))
        dict_news['artist'] = artist
        dict_news['song_num'] = col1.find({'song_artist': artist_mc}).distinct('num')

        pass
    # except:
    #     print('%s, %s, 패스' % (artist, date))
    #         dict_news['artist'] = artist
    #         dict_news['song_num'] = col1.find({'song_artist': artist_mc}).distinct('num')
    #
    #     pass


client = MongoClient('localhost', 27017)
db1 = client.music_cow
col1 = db1.musicCowData
db2 = client.article
col5 = db2.article_info
date = (datetime.today() - relativedelta(months=3)).strftime('%Y-%m-%d')

if __name__ == '__main__':
    artist = '송가인'
    df_artist_nlp = df_nlp()
    bertopic(artist, date)

