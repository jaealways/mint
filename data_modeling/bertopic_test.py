import pandas as pd
from tqdm import tqdm
from bertopic import BERTopic
from datetime import datetime
from pymongo import MongoClient
from sklearn.feature_extraction.text import CountVectorizer

from data_modeling.nlp_modeling import NLPModeling
from data_transformation.db_env import DbEnv, db
from data_crawling.artist_for_nlp import df_nlp


# date = (datetime.today() - relativedelta(months=6)).strftime('%Y-%m-%d')

client = MongoClient('localhost', 27017)
db1 = client.music_cow
col1 = db1.musicCowData

date = '2021-04-19'
artist = '브레이브걸스'
df_artist_nlp = df_nlp()
artist_mongo = df_artist_nlp[df_artist_nlp['nlp_dict'] == artist]['music_cow'].values[0]

print(date)
conn, cursor = DbEnv().connect_sql()
list_tokens, list_time = NLPModeling().import_token_bert(conn, cursor, date, artist)

vectorizer = CountVectorizer(input=list_tokens, max_features=3000)
model = BERTopic(embedding_model="sentence-transformers/xlm-r-100langs-bert-base-nli-stsb-mean-tokens", \
                 vectorizer_model=vectorizer,
                 nr_topics=10,
                 top_n_words=20,
                 calculate_probabilities=True)

# topics = model.fit(list_tokens)
# model.save("%s_%s" % (artist, num_mongo_list))
# print('모델 저장 완료')

# topics = model.fit(list_tokens)
model_after = model.load("%s" % artist)
print('모델 출력 완료1')

# topics = model.get_topic_info()
# print('모델 출력 완료2')
# df_time = model.topics_over_time(list_tokens, topics, list_time)
# df_time.to_pickle("df_%s" % artist)
# #
# # # model.visualize_topics()
print('모델 출력 완료3')
df_time = pd.read_pickle("df_%s" % artist)
fig = model.visualize_topics_over_time(df_time)
#
fig.write_html("file.html")
# # model.visualize_topics()
# # model.visualize_distribution(probs[0])

# num_mongo = col1.find({'song_artist': artist_mongo}).distinct('num')
# num_mongo_list = "_".join([str(x) for x in num_mongo])
