# << 메인 크롤링 코드 >>
# 작성자 : 정예원
#
# [ 코드 설명 ]
# <Track 1> , <Track 2> , <Track 3> 으로 이루어져 멀티 프로세싱 방식으로 진행되어야 합니다.
#
# <Track 1> - musicCowCrawler3 , musicInfoCrawler, copyrigihtPriceCrawler, naverCrawler 로 이루어짐.
# : 현재 musicCowData 에 있는 곡들을 대상으로 'musicCow 크롤링' , 'musicInfo 크롤링' , 'copyright 크롤링' , 'naver 크롤링' 을 병렬적으로 진행합니다.
#
# <Track 2> - songCralwerNew, songSeparator
# : 2000 ~ 3000 사이의 신곡을 탐색하고, 가수명과 곡명에 split 을 적용합니다.
#
# <Track 3> - mcpiCrawler 로 이루어짐
# : mcpi 지수를 크롤링 합니다.
#
# [아직 못한 것들]
# 가수명/노래제목 split 적용, copyrightCrawler 디비 연결 오류 , naverCrawler 연결, songCrawlerNew, songSeparator 미완성, multiprocessing

# modules
import datetime
import schedule
import time
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient
import os
from multiprocessing import Process, Pool
import numpy as np
import math
from tqdm import tqdm


from crawlers import musicCowCrawler, songSeparator, copyrightCrawler, musicInfoCrawler, mcpiCrawler
from data_crawling import artist_for_nlp
from data_preprocessing.nlp_tokenizing import article_to_token, update_mecab_dict_nnp, update_mecab_dict_person, update_powershell
from data_crawling.crawler_naver_news_link import listing_article
from data_crawling.crawler_naver_news_text import text_crawler
from data_crawling.genie_genre_crawler import genie_genre
from data_transformation.mongo_to_sql import MongoToSQL
from data_crawling.artist_for_nlp import list_artist_NNP
from data_modeling.bertopic_text import bertopic

# == 몽고디비 ==
client = MongoClient('localhost', 27017)
db1 = client.music_cow
db2 = client.article

# music cow
col1 = db1.musicCowData
col2 = db1.mcpi
col3 = db1.copyright_price
col4 = db1.musicInfo

# article
col5 = db2.article_info
dateToday = datetime.datetime.today()


def track1(SongNumListCurrent):
    # ====================================== << Track 1 >> : 현재 musicCowData 디비에 있는 곡들 기준 크롤링 =========================================
    # 1. 현재 musicCowData 디비에 있는 곡들 / 가수들
    # 1-1. DB에 있는 곡들 대상으로 뮤직카우 데이터 크롤링
    print("<< track1 시작 >>")
    print("<< 뮤직카우 데이터 크롤링을 시작합니다 >>")
    musicCowCrawler.songCrawler(col1, SongNumListCurrent)  # 뮤직카우 디비에 있는 기존 곡들 크롤링

    # 1-2. copyrightPrice 크롤링
    print("<< 저작권료 크롤링을 시작합니다 >> ")
    copyrightCrawler.copyrightCrawler(col3, dateToday, SongNumListCurrent)

    # 1-3. mcpi 크롤링
    print("<< mcpi 크롤링을 시작합니다 >> ")
    mcpiCrawler.mcpiCrawler(col2)  # mcpi 지수 크롤링

    # 1-4. mongoDB to SQL 크롤링
    print("<<  mongoDB to SQL을 시작합니다 >> ")
    MongoToSQL().update_daily_music_cow()
    MongoToSQL().update_daily_mcpi()


    ##### 기존 곡 분석 모델
    # 2-1. 시총 계산
    print("<< 시가총액 계산을 시작합니다 >> ")


    # 2-2. PER 계산
    print("<< PER 계산을 시작합니다 >> ")


    # 2-3. 베타 계산
    print("<< 베타 계산을 시작합니다 >> ")


    # 2-4. 공포탐욕지수 계산
    print("<< 공탐지수 계산을 시작합니다 >> ")

    # 2-5. 턴오버 계산
    print("<< 턴오버 계산을 시작합니다 >> ")



    # 4-2. newSong 크롤링
    print("<< 신곡 크롤링을 시작합니다 >>")
    newSongList = musicCowCrawler.songCrawlerNew(col1, SongNumListCurrent)
    print(newSongList)
    newArtistList = songSeparator.SongSeparator(col1)
    print(newArtistList)

    # 4-3. musicInfo 크롤링
    print("<< 곡 information 크롤링을 시작합니다 >> ")
    musicInfoCrawler.musicInfoCrawler(col1, col4)

    # 4-4. copyrightPrice 크롤링
    print("<< 저작권료 크롤링을 시작합니다 >> ")
    copyrightCrawler.copyrightCrawler(col3, dateToday, newSongList)

    # 4-5. genre 크롤링
    print("<< genre 크롤링을 시작합니다 >> ")
    genie_genre()


def track2(NewsArtistListCurrent):
    # ======================================================== << Track 2 >> : 기사 크롤링 ==================================================
    # np.nan이 artist list에 어떤 부분에 들어갔나?? 몽고디비에 저장되어있었음
    print("<< track2 시작 >>")
    print("<< Naver 크롤링을 시작합니다 >> ")
    pool = Pool(10)
    pool.map(listing_article, NewsArtistListCurrent)
    #
    print("<< Naver 크롤링 링크 기록을 시작합니다 >> ")
    articles = list(col5.find({'date_crawler': dateToday.strftime('%Y-%m-%d')}))
    article_num = col5.count_documents({'date_crawler': {'$lt': dateToday.strftime('%Y-%m-%d')}}) + 1
    [col5.update_one({'_id': val['_id']}, {'$set': {'doc_num': idx+article_num}}) for idx, val in enumerate(articles)]
    #
    print("<< Naver 본문 크롤링을 시작합니다 >> ")
    articles = list(col5.find({'date_crawler': dateToday.strftime('%Y-%m-%d'), 'text': {'$exists': False}}))
    pool.map(text_crawler, articles)

    print('<< NLP 사전 업데이트를 시작합니다 >> ')
    update_mecab_dict_nnp()
    update_mecab_dict_person()
    update_powershell()

    print("<< NLP 전처리 시작합니다 >> ")
    articles = list(col5.find({'date_crawler': dateToday.strftime('%Y-%m-%d')}))
    print("NLP 개수", len(articles))
    article_to_token(articles)


def track3():
    # ======================================================== NLP 분석
    print("<< NLP 토픽 모델링 시작합니다 >>")
    pool = Pool(8)

    list_artist = list_artist_NNP
    pool.map(bertopic, list_artist)

    # for artist in tqdm(list_artist):
    #     bertopic(artist)


def multi_process():
    # print("{0} 크롤링 시작합니다".format(dateToday.strftime('%Y-%m-%d')))
    # SongNumListCurrent = list(col1.find({}, {'num': {"$slice": [1, 1]}}))
    # NewsArtistListCurrent = artist_for_nlp.list_artist_query
    # NewsArtistListCurrent = [x for x in NewsArtistListCurrent if str(x) != 'nan']
    # track2(NewsArtistListCurrent,)

    print("{0} 분석 시작합니다".format(dateToday.strftime('%Y-%m-%d')))
    # track1(SongNumListCurrent,)
    track3()

    print('끝')

    # pl = Pool(2)
    #
    # pl.apply_async(track1, (SongNumListCurrent,))
    # pl.apply_async(track3)
    #
    # pl.close()
    # pl.join()


if __name__ == '__main__':
    start = time.time()
    print("{0} 작업 시작합니다".format(dateToday.strftime('%Y-%m-%d')))

    # artist_for_nlp

    # === 크롤링 ===

    with open("storage/check_new/newArtistList.txt", 'w') as f:
        pass
    with open("storage/check_new/newSongList.txt", 'w') as f:
        pass

    multi_process()
    # schedule.every().day.at("00:01").do(multi_process)

    while True:
        schedule.run_pending()
        time.sleep(1)



