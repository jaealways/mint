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


def bertopic(artist):
    dict_news = {}
    artist_mc = df_artist_nlp[df_artist_nlp['nlp_dict'] == artist]['music_cow'].values[0]
    artist_query = df_artist_nlp[df_artist_nlp['nlp_dict'] == artist]['nlp_query'].values[0]

    dict_news['artist'] = artist
    dict_news['song_num'] = col1.find({'song_artist': artist_mc}).distinct('num')
    dict_news['date'] = datetime.today().strftime('%Y-%m-%d')

    conn, cursor = DbEnv().connect_sql()

    try:
        list_tokens, list_time, list_doc_num = NLPModeling().import_token_bert(conn, cursor, date, artist_mc)

        topic_num = round(len(list_tokens) ** 0.25)
        print('%s, %s, 주제 %s개, 기사 %s개' % (artist, date, topic_num, len(list_tokens)))
        vectorizer = CountVectorizer(input=list_tokens, max_features=3000)
        model = BERTopic(embedding_model="sentence-transformers/xlm-r-100langs-bert-base-nli-stsb-mean-tokens", \
                         vectorizer_model=vectorizer, nr_topics=topic_num, top_n_words=20, calculate_probabilities=True)

        topics, _ = model.fit_transform(list_tokens)
        repre_docs = model.representative_docs

        for k, v in repre_docs.items():
            news_text = max(v, key=len)
            index_text = list_tokens.index(news_text)
            doc_num_text = list_doc_num[index_text]
            date_text = list_time[index_text]
            link_text = col5.find({'artist': artist_query, 'date': date_text, 'doc_num': int(doc_num_text)}).distinct('link')
            for k_l in link_text:
                if k_l == ' ':
                    pass
                else:
                    dict_news[str(k)] = k_l
                    break

        df_time = model.topics_over_time(list_tokens, topics, list_time, nr_bins=20)
        fig = model.visualize_topics_over_time(df_time, top_n_topics=10)

        col7.insert_one(dict_news).inserted_id

        fig.write_html("./storage/dict_artist/%s.html" % artist)

        return dict_news

    except:
        print('%s, %s, 패스' % (artist, date))
        col7.insert_one(dict_news).inserted_id
        pass


client = MongoClient('localhost', 27017)
db1 = client.music_cow
col1 = db1.musicCowData
db2 = client.article
col5 = db2.article_info
col7 = db1.newsLink
date = (datetime.today() - relativedelta(months=3)).strftime('%Y-%m-%d')
df_artist_nlp = df_nlp()
list_artist = df_artist_nlp['nlp_dict'].values.tolist()

if __name__ == '__main__':

    # artist = '뮤직카우'
    # bertopic(artist, date)

    for artist in tqdm(list_artist):
        bertopic(artist)

