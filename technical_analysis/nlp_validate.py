import pandas as pd
from pymongo import MongoClient

from data_transformation.db_env import DbEnv, db
from data_modeling.nlp_modeling import NLPModeling
from bertopic import BERTopic
from datetime import datetime
from pymongo import MongoClient
from sklearn.feature_extraction.text import CountVectorizer
from dateutil.relativedelta import relativedelta
import nltk
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from contextualized_topic_models.models.ctm import CombinedTM
from contextualized_topic_models.utils.data_preparation import TopicModelDataPreparation
from contextualized_topic_models.utils.preprocessing import WhiteSpacePreprocessing
import nltk


def validate_lsa(list_tokens, topic_num):
    vectorizer = TfidfVectorizer(max_features=1000, max_df=0.5, smooth_idf=True)
    df_token = pd.DataFrame(list_tokens)
    X = vectorizer.fit_transform(df_token[0])

    print('TF-IDF 행렬의 크기 :', X.shape)

    svd_model = TruncatedSVD(n_components=topic_num, algorithm='randomized', n_iter=100, random_state=122)
    svd_model.fit(X)
    len(svd_model.components_)

    terms = vectorizer.get_feature_names()  # 단어 집합. 1,000개의 단어가 저장됨.

    for idx, topic in enumerate(svd_model.components_):
        print("Topic %d:" % (idx + 1), [(terms[i], topic[i].round(5)) for i in topic.argsort()[:-5 - 1:-1]])


def test_lda(list_tokens, topic_num):
    vectorizer = TfidfVectorizer(max_features=1000, max_df=0.5, smooth_idf=True)
    df_token = pd.DataFrame(list_tokens)
    X = vectorizer.fit_transform(df_token[0])

    print('TF-IDF 행렬의 크기 :', X.shape)

    lda_model = LatentDirichletAllocation(n_components=topic_num, learning_method='online', random_state=777, max_iter=1)

    lda_top = lda_model.fit_transform(X)
    terms = vectorizer.get_feature_names()

    for idx, topic in enumerate(lda_model.components_):
        print("Topic %d:" % (idx+1), [(terms[i], topic[i].round(2)) for i in topic.argsort()[:-10 - 1:-1]])


def validate_bertopic(list_tokens, topic_num):
    vectorizer = CountVectorizer(input=list_tokens, max_features=3000)
    model = BERTopic(embedding_model="sentence-transformers/xlm-r-100langs-bert-base-nli-stsb-mean-tokens", \
                     vectorizer_model=vectorizer, nr_topics=topic_num, top_n_words=10, calculate_probabilities=True)

    topics, _ = model.fit_transform(list_tokens)
    model.get_topic_info()
    model.visualize_topics()


client = MongoClient('localhost', 27017)
db1 = client.music_cow
db2 = client.article
col5 = db2.article_info

conn, cursor = DbEnv().connect_sql()

date_start, date_end = '2021-07-01', '2021-09-31'
artist = '브레이브걸스'
list_tokens = NLPModeling().import_token_test_set(conn, cursor, date_start, date_end, artist)

topic_num = round(len(list_tokens) ** 0.25)
print('%s, 주제 %s개, 기사 %s개' % (artist, topic_num, len(list_tokens)))

validate_lsa(list_tokens, topic_num)

