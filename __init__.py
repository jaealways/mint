# modules
from datetime import datetime, timedelta
import schedule
import time
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient
import os
from multiprocessing import Process, Pool
import numpy as np
import math
from tqdm import tqdm
from selenium import webdriver
import pandas as pd

from crawlers import musicCowCrawler, songSeparator, copyrightCrawler, musicInfoCrawler, mcpiCrawler
from data_crawling import artist_for_nlp
from data_preprocessing.nlp_tokenizing import article_to_token, update_mecab_dict_nnp, update_mecab_dict_person, update_powershell
from data_crawling.crawler_naver_news_link import listing_article
from data_crawling.crawler_naver_news_text import text_crawler
from data_crawling.genie_genre_crawler import genie_genre
from data_transformation.mongo_to_sql import MongoToSQL
from data_crawling.artist_for_nlp import list_artist_NNP
from data_modeling.bertopic_text import bertopic
from data_preprocessing.data_tidying import DataTidying
from data_modeling.song_analytics import SongAnalytics
from data_preprocessing.web_update import *

# == 몽고디비 ==
client = MongoClient('localhost', 27017)
db1 = client.music_cow
db2 = client.article
db3 = client.musicowlab

# music cow
col1 = db1.musicCowData
col2 = db1.mcpi
col3 = db1.copyright_price
col4 = db1.musicInfo
col7 = db1.newsLink

# article
col5 = db2.article_info
col6 = db2.article_info_history

# web
col8 = db3.mcpi_info
col9 = db3.song_info
col10 = db3.index_rank


# dateToday = datetime.datetime.today()


def track1(NewsArtistListCurrent):
    # ======================================================== << Track 2 >> : 기사 크롤링 ==================================================
    print("<< track1 시작 >>")
    print("<< Naver 크롤링을 시작합니다 >> ")
    pool = Pool(20)
    pool.map(listing_article, NewsArtistListCurrent)

    print("<< Naver 크롤링 링크 기록을 시작합니다 >> ")
    articles = list(col5.find({'date_crawler': datetime.today().strftime('%Y-%m-%d')}))
    article_num = col6.count_documents({'date_crawler': {'$lt': datetime.today().strftime('%Y-%m-%d')}}) + 1
    [col5.update_one({'_id': val['_id']}, {'$set': {'doc_num': idx+article_num}}) for idx, val in enumerate(articles)]

    print("<< Naver 본문 크롤링을 시작합니다 >> ")
    articles = list(col5.find({'date_crawler': datetime.today().strftime('%Y-%m-%d'), 'text': {'$exists': False}}))
    pool.map(text_crawler, articles)

    print("<< article info에서 history로 이동합니다 >> ")
    articles = list(col5.find({'date_crawler': datetime.today().strftime('%Y-%m-%d')}))
    col6.insert_many(articles).inserted_ids
    date_3m = (datetime.today() - relativedelta(months=3)).strftime('%Y-%m-%d')
    col5.delete_many({"date": {"$lt": date_3m}})
    MongoToSQL().delete_article_3m(date_3m)

    print('<< NLP 사전 업데이트를 시작합니다 >> ')
    update_mecab_dict_nnp()
    update_mecab_dict_person()
    update_powershell()

    print("<< NLP 전처리 시작합니다 >> ")
    articles = list(col5.find({'date_crawler': datetime.today().strftime('%Y-%m-%d')}))
    print("NLP 개수", len(articles))
    article_to_token(articles)


def track2():
    # ======================================================== NLP 분석
    print("<< track2 시작 >>")
    print("<< NLP 토픽 모델링 시작합니다 >>")
    # 토픽 모델링 진행할 아티스트를 7개로 나누는 로직
    pool = Pool(6)
    pool_date = 2
    date_7d = (datetime.today() - timedelta(days=pool_date)).strftime('%Y-%m-%d')
    list_exist_day_artist = col7.find({'date': {'$gt': date_7d}}).distinct('artist')
    list_exist_day_num = col7.find({'date': {'$gt': date_7d}}).distinct('date')
    list_artist_full = list(set(list_artist_NNP) - set(list_exist_day_artist))
    list_artist = list_artist_full[:round(len(list_artist_full)/(pool_date-len(list_exist_day_num)))]
    # list_artist = list_artist_full
    pool.map(bertopic, list_artist)

def track3(SongNumListCurrent):
    # ====================================== << Track 1 >> : 현재 musicCowData 디비에 있는 곡들 기준 크롤링 =========================================
    # 1. 현재 musicCowData 디비에 있는 곡들 / 가수들
    # 3-1. DB에 있는 곡들 대상으로 뮤직카우 데이터 크롤링
    # 크롤링 멀티프로세싱으로 바꾸고, 분석은 sync apply로
    pool = Pool(20)
    print("<< track3 시작 >>")
    print("<< 뮤직카우 데이터 크롤링을 시작합니다 >>")
    pool.map(musicCowCrawler.songCrawler, SongNumListCurrent)
    # musicCowCrawler.songCrawler(SongNumListCurrent)  # 뮤직카우 디비에 있는 기존 곡들 크롤링
    col1.update_many({}, {'$unset': {datetime.today().strftime('%Y-%m-%d'): ''}})

    # # 3-2. copyrightPrice 크롤링
    if datetime.today().day == 1:
        print("<< 저작권료 크롤링을 시작합니다 >> ")
        copyrightCrawler.copyrightCrawler(SongNumListCurrent)

    # 3-3. mcpi 크롤링
    print("<< mcpi 크롤링을 시작합니다 >> ")
    mcpiCrawler.mcpiCrawler(col2)  # mcpi 지수 크롤링

    # 3-4. mongoDB to SQL 크롤링
    print("<<  mongoDB to SQL을 시작합니다 >> ")
    MongoToSQL().update_daily_music_cow()
    MongoToSQL().update_daily_mcpi()

    # pl = Pool(3)
    #
    # print("<< mcpi 크롤링을 시작합니다 >> ")
    # pl.apply_async(mcpiCrawler.mcpiCrawler, (col2, ))
    #
    # print("<<  mongoDB to SQL을 시작합니다 >> ")
    # pl.apply_async(MongoToSQL().update_daily_music_cow())
    # pl.apply_async(MongoToSQL().update_daily_mcpi())
    #
    # pl.close()
    # pl.join()

def track4():
    ##### 기존 곡 분석 모델
    print("<< track4 시작 >>")
    pool = Pool(20)
    duration = 365
    list_song_num = col1.find({}).distinct('num')
    date_df = (datetime.today() - timedelta(days=duration+2)).strftime('%Y-%m-%d')

    df_price = pd.DataFrame()
    df_price_temp = pool.map(DataTidying().get_df_price, list_song_num)
    for i in df_price_temp:
        df_price = pd.concat([df_price, i], axis=1)
    df_price = df_price.T

    df_price_volume = pd.DataFrame()
    df_volume_temp = pool.map(DataTidying().get_df_price_volume, list_song_num)
    for i in df_volume_temp:
        df_price_volume = pd.concat([df_price_volume, i], axis=1)
    df_price_volume = df_price_volume.T

    df_mcpi = DataTidying().get_df_mcpi(date_df)
    df_mcpi_volume = DataTidying().get_df_mcpi_volume(date_df)
    df_copyright = DataTidying().get_df_copyright()
    df_stock = DataTidying().get_df_stock_num()

    # 2-1. 시총 계산
    print("<< 시가총액 계산을 시작합니다 >> ")
    SongAnalytics().market_cap(df_price.iloc[:,1:], df_stock)

    # 2-2. PER 계산
    print("<< PER 계산을 시작합니다 >> ")
    SongAnalytics().per_duration(df_price.iloc[:,1:], df_copyright)

    # 2-3. 베타 계산
    print("<< 베타 계산을 시작합니다 >> ")
    SongAnalytics().beta_index(df_price.iloc[:,1:], df_mcpi.iloc[:,1:])

    # 2-4. 공포탐욕지수 계산
    print("<< MCPI 공탐지수 계산을 시작합니다 >> ")
    SongAnalytics().fng_index(df_mcpi, df_mcpi_volume)

    print("<< 곡 공탐지수 계산을 시작합니다 >> ")
    SongAnalytics().fng_index(df_price, df_price_volume, duration=120)

    # 2-5. 턴오버 계산
    print("<< 1년 턴오버 계산을 시작합니다 >> ")
    SongAnalytics().turn_over(df_price_volume, df_stock)

def track5(SongNumListCurrent):
    print("<< track5 시작 >>")
    # 5-2. newSong 크롤링
    print("<< 신곡 크롤링을 시작합니다 >>")
    newSongList = musicCowCrawler.songCrawlerNew(col1, SongNumListCurrent)
    print(newSongList)
    newArtistList = songSeparator.SongSeparator(col1)
    print(newArtistList)

    # 5-3. musicInfo 크롤링
    print("<< 곡 information 크롤링을 시작합니다 >> ")
    musicInfoCrawler.musicInfoCrawler(col1, col4)

    # 5-4. copyrightPrice 크롤링
    print("<< 저작권료 크롤링을 시작합니다 >> ")
    copyrightCrawler.copyrightCrawler(newSongList)

    # 5-5. genre 크롤링
    print("<< genre 크롤링을 시작합니다 >> ")
    genie_genre()

def track6():
    print("<< track6 시작 >>")
    print("<< 업데이트 정보를 정리합니다 >>")
    pool = Pool(20)

    print("<< MCPI 웹 정보를 정리합니다 >>")
    update_mcpi_info()

    print("<< 노래 웹 정보를 정리합니다 >>")
    list_song = list(col4.find({}))
    pool.map(update_song_info, list_song)

    print("<< NLP 웹 정보를 정리합니다 >>")
    list_song_nlp1 = list(col7.find({'date': (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')}))
    pool.map(update_song_info_nlp, list_song_nlp1)
    list_song_nlp2 = list(col7.find({'date': (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')}))
    pool.map(update_song_info_nlp, list_song_nlp2)

    print("<< 순위 웹 정보를 정리합니다 >>")
    update_rank_info()


def track7():
    print("<< track7 시작 >>")
    print("<< 웹 업데이트를 시작합니다 >>")


def multi_process():
    print("{0} 크롤링 시작합니다".format(datetime.today().strftime('%Y-%m-%d')))
    SongNumListCurrent = list(col1.find({}, {'num': {"$slice": [1, 1]}}))
    NewsArtistListCurrent = artist_for_nlp.list_artist_query
    NewsArtistListCurrent = [x for x in NewsArtistListCurrent if str(x) != 'nan']
    track1(NewsArtistListCurrent,)

    print("{0} 분석 시작합니다".format(datetime.today().strftime('%Y-%m-%d')))
    track2()
    track3(SongNumListCurrent,)
    track4()
    track5(SongNumListCurrent,)

    print("{0} 웹페이지 업데이트 시작합니다".format(datetime.today().strftime('%Y-%m-%d')))
    track6()
    track7()

    print('끝')
    print(datetime.now())


if __name__ == '__main__':
    start = time.time()
    print("{0} 작업 시작합니다".format(datetime.today().strftime('%Y-%m-%d')))

    with open("storage/check_new/newArtistList.txt", 'w') as f:
        pass
    with open("storage/check_new/newSongList.txt", 'w') as f:
        pass

    # multi_process()
    schedule.every().day.at("00:01").do(multi_process)

    while True:
        schedule.run_pending()
        time.sleep(1)



