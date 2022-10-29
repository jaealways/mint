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
    list_tokens, list_time, list_doc_num = NLPModeling().import_token_bert(conn, cursor, date_3m, artist)

    try:
        topic_num = round(len(list_tokens) ** 0.25)
        # topic_num = proper_topic_num()
        print('%s, %s, 주제 %s개, 기사 %s개' % (artist, date_3m, topic_num, len(list_tokens)))
        vectorizer = CountVectorizer(input=list_tokens, max_features=3000)
        model = BERTopic(embedding_model="sentence-transformers/xlm-r-100langs-bert-base-nli-stsb-mean-tokens", \
                         vectorizer_model=vectorizer, nr_topics=topic_num, top_n_words=10, calculate_probabilities=True)

        topics, _ = model.fit_transform(list_tokens)
        repre_docs = model.representative_docs

        for k, v in model.topics.items():
            list_keyword = [x[0] for x in v]
            keyword = ', '.join(list_keyword)
            tuple_insert = (artist, k, dateyes, keyword)
            sql = f"""INSERT INTO mu_tech.topickeyword (artist, topic, date, keyword) VALUES {tuple_insert}"""
            cursor.execute(sql)
            conn.commit()

        for k, v in repre_docs.items():
            doc_count = 0
            news_text = max(v, key=len)
            index_text = list_tokens.index(news_text)
            doc_num_text = list_doc_num[index_text]
            date_text = list_time[index_text]
            link_text = col5.find({'artist': artist_query, 'date': date_text, 'doc_num': int(doc_num_text)})
            for val in link_text:
                if val['link'] == ' ':
                    pass
                else:
                    if doc_count == 5:
                        break
                    tuple_insert = (artist, dateyes, str(k), val['link'], val['article_title'], val['date'])
                    sql = f"""INSERT INTO mu_tech.topicnews (artist, date, topicnum, link, title, date_news) VALUES {tuple_insert}"""
                    doc_count += 1
                    cursor.execute(sql)
                    conn.commit()

        df_time = model.topics_over_time(list_tokens, topics, list_time, nr_bins=20)
        fig = model.visualize_topics_over_time(df_time, top_n_topics=10, width=900)
        fig.layout.dragmode = False

        fig.write_html("storage/dict_artist/%s.html" % artist)

        with open("storage/dict_artist/%s.html" % artist, "r", encoding='utf-8') as f:
            code_html = f.read()
            code_html = code_html.replace('height:450px; width:900px', 'height:50vh; width:80vw')
            tuple_insert = (artist, dateyes, len(list_tokens), code_html)
            sql = f"""INSERT INTO mu_tech.topicmodel (artist, date, len_news, code_html) VALUES {tuple_insert}"""
            cursor.execute(sql)
            conn.commit()

    except:
        print('%s, %s, 패스' % (artist, date_3m))
        code_html = '<h4 align="center">곡 관련 기사(3개월 간 80개)가 적어 토픽 모델링을 진행할 수 없습니다.</h4>'
        tuple_insert = (artist, dateyes, len(list_tokens), code_html)
        sql = f"""INSERT INTO mu_tech.topicmodel (artist, date, len_news, code_html) VALUES {tuple_insert}"""
        cursor.execute(sql)
        conn.commit()
        pass


client = MongoClient('localhost', 27017)
db1 = client.music_cow
col1 = db1.musicCowData
db2 = client.article
col5 = db2.article_info
col7 = db1.newsLink
date_3m = (datetime.today() - relativedelta(months=3)).strftime('%Y-%m-%d')
dateyes = (datetime.today() - relativedelta(days=1)).strftime('%Y-%m-%d')

df_artist_nlp = df_nlp()
list_artist = df_artist_nlp['nlp_dict'].values.tolist()
conn, cursor = DbEnv().connect_sql()
from data_crawling.artist_for_nlp import list_artist_NNP

def proper_topic_num():
    # bert 분석 통해서 뉴스 갯수별로 적절한 확률변수 생성하기
    topic_num=1
    return topic_num


if __name__ == '__main__':
    # sql = f"SELECT distinct artist from topicmodel WHERE date >= '2022-06-01'"
    # cursor.execute(sql)
    # result = cursor.fetchall()
    # list_exist_day_artist = [x[0] for x in result]
    #
    # list_artist_full = list(set(list_artist_NNP) - set(list_exist_day_artist))
    #
    # for artist in list_artist_full:
    #     bertopic(artist)

    artist = '뮤직카우'
    bertopic(artist)


    # for artist in tqdm(list_artist):
    #     bertopic(artist)

