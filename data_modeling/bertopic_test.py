from tqdm import tqdm
from sklearn.feature_extraction.text import CountVectorizer
from konlpy.tag import Mecab
from bertopic import BERTopic

from data_modeling.nlp_modeling import NLPModeling
from data_transformation.db_env import DbEnv, db


# class CustomTokenizer:
#     def __init__(self, tagger):
#         self.tagger = tagger
#     def __call__(self, sent):
#         sent = sent[:1000000]
#         word_tokens = self.tagger.morphs(sent)
#         result = [word for word in word_tokens if len(word) > 1]
#         return result


# from sklearn.datasets import fetch_20newsgroups
#
# docs = fetch_20newsgroups(subset='all',  remove=('headers', 'footers', 'quotes'))['data']

date = '2022-01-20'
conn, cursor = DbEnv().connect_sql()
list_tokens, list_time = NLPModeling().import_token_bert(conn, cursor, date)

# custom_tokenizer = CustomTokenizer(Mecab())
vectorizer = CountVectorizer(input=list_tokens, max_features=3000)
model = BERTopic(embedding_model="sentence-transformers/xlm-r-100langs-bert-base-nli-stsb-mean-tokens", \
                 vectorizer_model=vectorizer,
                 nr_topics=50,
                 top_n_words=10,
                 calculate_probabilities=True)
topics, probs = model.fit_transform(list_tokens)
model.topics_over_time(list_tokens, topics, list_time)
model.visualize_topics()
model.visualize_distribution(probs[0])

model().save("bert")
