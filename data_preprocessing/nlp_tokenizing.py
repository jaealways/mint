import numpy
from pymongo import MongoClient
import pandas as pd
import itertools
from konlpy.tag import Mecab
import numpy as np
from collections import Counter
import csv
import re
import time
from bs4 import BeautifulSoup
from tqdm import tqdm
import subprocess, sys
from jamo import h2j, j2hcj
import datetime

from data_transformation.db_env import DbEnv, db
import data_crawling.artist_event_rule as aer
import data_crawling.artist_for_nlp as afn


client = MongoClient('localhost', 27017)
db1 = client.music_cow
db2 = client.article
col1 = db1.musicCowData
col5 = db2.article_info
conn, cursor = DbEnv().connect_sql()
date_crawler = datetime.datetime.today().strftime('%Y-%m-%d')

mecab = Mecab(dicpath=r'C:\mecab\mecab-ko-dic')


def db_to_article(list_article):
    df_artist_nlp = afn.df_nlp()
    # articles1 = list(col.find({'text': {'$exists': True}}))

    title1 = list(map(lambda x: x['article_title'], list_article))
    text1 = list(map(lambda x: x['text'], list_article))
    text2 = list(map(lambda x, y: '%s. %s' % (x, y), title1, text1))
    text2 = list(map(lambda x: text_cleaning(x), text2))
    artist1 = list(map(lambda x: x['artist'], list_article))
    docnum1 = list(map(lambda x: x['doc_num'], list_article))

    # list_temp = set(artist1) - set(df_artist_nlp['nlp_query'].values)

    artist2 = list(map(lambda x: df_artist_nlp[df_artist_nlp['nlp_query'] == x]['nlp_dict'].values[0], artist1))
    date1 = list(map(lambda x: x['date'], list_article))
    date2 = list(map(lambda x: x['date_crawler'], list_article))

    list_article = list(zip(text2, docnum1, artist2, date1, date2))
    list_article = [x for x in list_article if x[0] not in (' .  ', '. ')]
    print("list_article")

    return list_article


def article_to_sen(list_article):
    article_split = list(map(lambda x: x[0].split('. '), list_article))
    list(map(lambda x, y: x.append(y[0]), article_split, list_article))
    len_article = list(map(lambda x: len(x), article_split))

    list_sen, list_artist, list_date, list_doc = [], [], [], []

    list(map(lambda x: list_sen.extend(x), article_split))
    list(map(lambda x, y: list_doc.extend([x[1] for z in range(y)]), list_article, len_article))
    list(map(lambda x, y: list_artist.extend([x[2] for z in range(y)]), list_article, len_article))
    list(map(lambda x, y: list_date.extend([x[3] for z in range(y)]), list_article, len_article))

    list_sen = list(map(lambda x: text_cleaning(x), list_sen))
    list_date_crawler = [date_crawler for x in range(len(list_sen))]

    list_sens_temp = list(zip(list_sen, list_doc, list_artist, list_date, list_date_crawler))
    sql = "INSERT INTO newssen VALUES (%s, %s, %s, %s, %s)"
    list_sens = list(map(lambda x: sql_exclude(x, sql), list_sens_temp))
    print("list_sens")

    return list_sens


def article_to_token(list_article):
    df_artist_nlp = afn.df_nlp()
    # articles1 = list(col.find({'text': {'$exists': True}}))

    title1 = list(map(lambda x: x['article_title'], list_article))
    text1 = list(map(lambda x: x['text'], list_article))
    text2 = list(map(lambda x, y: '%s. %s' % (x, y), title1, text1))
    text2 = list(map(lambda x: text_cleaning(x), text2))
    artist1 = list(map(lambda x: x['artist'], list_article))
    docnum1 = list(map(lambda x: x['doc_num'], list_article))

    # list_temp = set(artist1) - set(df_artist_nlp['nlp_query'].values)

    artist2 = list(map(lambda x: df_artist_nlp[df_artist_nlp['nlp_query'] == x]['nlp_dict'].values[0], artist1))
    date1 = list(map(lambda x: x['date'], list_article))
    date2 = list(map(lambda x: x['date_crawler'], list_article))

    list_article = list(zip(text2, docnum1, artist2, date1, date2))
    list_article = [x for x in list_article if x[0] not in (' .  ', '. ')]

    array_token_temp = list(map(lambda x: np.asarray(mecab.pos(x[0])), list_article))
    array_token_tp = list(map(lambda x: x.T, array_token_temp))
    tuple_tokens = tuple(map(lambda x, y: sen_token_exclude(x, y), array_token_tp, list_article))

    sql = "INSERT INTO newssentoken VALUES (%s, %s, %s, %s, %s, %s)"
    list(map(lambda x: sql_exclude(x, sql), tuple_tokens))

    sql = "INSERT INTO newssentokenhistory VALUES (%s, %s, %s, %s, %s, %s)"
    list(map(lambda x: sql_exclude(x, sql), tuple_tokens))


def text_cleaning(text):
    text = re.sub('<a[ _a-zA-Z=\"\']+href.*?>(.*?)<\/a>', ' ', text)
    text = re.sub('<td[ _a-zA-Z=\"\']+.*?>(.*?)<\/td>', ' ', text)
    text = re.sub('<head.*/head>', ' ', text)
    text = re.sub('&[a-zA-Z0-9]+;', ' ', text)
    text = re.sub('<br />', ' ', text)
    text = BeautifulSoup(text).get_text()
    text = re.sub('[ ]+', ' ', text)
    return text


def sql_exclude(x, sql):
    try:
        cursor.execute(sql, x)
        conn.commit()
    except:
        print(x)
        pass
    else:
        return x


def sen_to_token():
    sql = "SELECT sen, doc_num, artist, date, date_crawler FROM newssen WHERE date_crawler = %s" % date_crawler
    cursor.execute(sql)
    conn.commit()
    list_sens = cursor.fetchall()

    array_token_temp = list(map(lambda x: np.asarray(mecab.pos(x[0])), list_sens))
    array_token_tp = list(map(lambda x: x.T, array_token_temp))
    tuple_tokens = tuple(map(lambda x, y: token_exclude_split(x, y), array_token_tp, list_sens))

    sql = "INSERT INTO newstoken VALUES (%s, %s, %s, %s, %s, %s)"
    list_tokens = list(map(lambda x: sql_exclude(x, sql), tuple_tokens))

    print('list_tokens')

    return list_tokens


def token_exclude_split(x, y):
    try:
        x[0]
    except:
        pass
    else:
        token, tag = ", ".join(x[0]), ", ".join(x[1])
        tuple_temp = (token, tag, y[1], y[2], y[3], y[4])
        return tuple_temp


def sen_token_exclude(x, y):
    token, tag = ", ".join(x[0]), ", ".join(x[1])
    tuple_temp = (token, tag, y[1], y[2], y[3], y[4])
    return tuple_temp


def sql_to_token():
    # sql = "SELECT token, tag, doc_num, artist, date, date_crawler FROM newstoken"
    sql = "SELECT token, tag, artist, date, date_crawler FROM newstoken"

    cursor.execute(sql)
    conn.commit()
    list_tokens = cursor.fetchall()

    list_token_temp = list(map(lambda x: np.asarray(x[0].split(', ')), list_tokens))
    list_tag = list(map(lambda x: np.asarray(x[1].split(', ')), list_tokens))
    list_token = list(map(lambda x, y: np.stack((x, y), axis=1), list_token_temp, list_tag))

    array_token = np.asarray([y[0] for x in list_token for y in x if y[1] == "NNP"])
    unique, counts = np.unique(array_token, return_counts=True)
    # dict_NNP = dict(zip(unique, counts))
    df_NNP = pd.DataFrame.from_dict([unique, counts]).T
    df_NNP = df_NNP.sort_values(by=[1], ascending=False)

    #
    return df_NNP


def contact_elements(huge_list):
    for i in huge_list:
        yield [list(itertools.chain(i[0], i[1], i[2])), i[3]]


def update_powershell():
    subprocess.run(["powershell", "-Command", "cd C:\\mecab"], capture_output=True)
    subprocess.run(["powershell", "-Command", ".\\tools\\add-userdic-win.ps1"], capture_output=True)
    subprocess.run(["powershell", "-Command", "Set-ExecutionPolicy Unrestricted"], capture_output=True)
    subprocess.run(["powershell", "-Command", ".\\tools\\add-userdic-win.ps1"], capture_output=True)
    subprocess.run(["powershell", "-Command", "!make install"], capture_output=True)


def get_jongsung_TF(sample_text):
    sample_text_list = list(sample_text)

    last_word = sample_text_list[-1]
    last_word_jamo_list = list(j2hcj(h2j(last_word)))
    last_jamo = last_word_jamo_list[-1]
    jongsung_TF = "T"
    if last_jamo in ['ㅏ', 'ㅑ', 'ㅓ', 'ㅕ', 'ㅗ', 'ㅛ', 'ㅜ', 'ㅠ', 'ㅡ', 'ㅣ', 'ㅘ', 'ㅚ', 'ㅙ', 'ㅝ', 'ㅞ', 'ㅢ', 'ㅐ,ㅔ', 'ㅟ', 'ㅖ',
                     'ㅒ']:
        jongsung_TF = "F"
    return jongsung_TF


def update_mecab_dict_nnp():
    list_db_title = list(col1.find({}).distinct("song_title"))

    with open('C:/mecab/user-dic/nnp.csv', 'r', newline='', encoding='utf8') as f:
        reader = csv.reader(f)
        list_nnp_dict = [row[0] for row in reader]
        f.close()

    set_total_title = set(aer.nnp_place) | set(aer.nnp_event) | set(aer.nnp_pro) | set(list_db_title)
    list_title_update = list(set_total_title - set(list_nnp_dict))
    print('업데이트 해야할 제목 리스트:')

    with open("C:/mecab/user-dic/nnp.csv", 'r', encoding='utf-8') as f:
        file_data = f.readlines()

    for word in list_title_update:
        print(word)
        jongsung_TF = get_jongsung_TF(word)
        line = '{},,,,NNP,*,{},{},*,*,*,*,*\n'.format(word, jongsung_TF, word)
        file_data.append(line)

    with open("C:/mecab/user-dic/nnp.csv", 'w', encoding='utf-8') as f:
        for line in file_data:
            f.write(line)


def update_mecab_dict_person():
    with open('C:/mecab/user-dic/person.csv', 'r', newline='', encoding='utf8') as f:
        reader = csv.reader(f)
        list_person_dict = [row[0] for row in reader]
        f.close()

    list_artist_db = list(col1.find({}).distinct("song_artist"))
    df_artist_nlp = pd.read_pickle("./storage/df_raw_data/df_artist_nlp.pkl")
    list_artist, list_to_update_df_dict = [], []

    for x in list_artist_db:
        try:
            value = df_artist_nlp[df_artist_nlp['music_cow'] == x]['nlp_dict'].values[0]
            list_artist.append(value)
        except:
            list_to_update_df_dict.append(x)

    print('데이터프레임 업데이트 해야할 아티스트 리스트: %s' % list_to_update_df_dict)

    set_total_artist = set(list_artist) | set(aer.nnp_artist)
    list_artist_update = list(set_total_artist - set(list_person_dict))
    list_artist_update = [x for x in list_artist_update if str(x) != 'nan']
    print('업데이트 해야할 아티스트 리스트:')

    with open("C:/mecab/user-dic/person.csv", 'r', encoding='utf-8') as f:
        file_data = f.readlines()

    for word in list_artist_update:
        print(word)
        jongsung_TF = get_jongsung_TF(word)
        line = '{},,,,NNP,*,{},{},*,*,*,*,*\n'.format(word, jongsung_TF, word)
        file_data.append(line)

    with open("C:/mecab/user-dic/person.csv", 'w', encoding='utf-8') as f:
        for line in file_data:
            f.write(line)
