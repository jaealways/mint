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


from crawlers import musicCowCrawler, songSeparator, copyrightCrawler, musicInfoCrawler, mcpiCrawler
from data_crawling import artist_for_nlp, crawler_naver_news_link, crawler_naver_news_text


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


def track1(SongNumListCurrent, start):
    # ====================================== << Track 1 >> : 현재 musicCowData 디비에 있는 곡들 기준 크롤링 =========================================
    # 1. 현재 musicCowData 디비에 있는 곡들 / 가수들
    # 1-1. DB에 있는 곡들 대상으로 뮤직카우 데이터 크롤링
    start1 = time.time()

    print("<< track2 시작 >>")
    print("<< 뮤직카우 데이터 크롤링을 시작합니다 >>")
    musicCowCrawler.songCrawler(col1, SongNumListCurrent)  # 뮤직카우 디비에 있는 기존 곡들 크롤링
    time_musicCow = time.time()

    # 1-2. copyrightPrice 크롤링
    print("<< 저작권료 크롤링을 시작합니다 >> ")
    copyrightCrawler.copyrightCrawler(col3, dateToday, SongNumListCurrent)
    time_copyrightPrice = time.time()

    # 1-3. mcpi 크롤링
    print("<< mcpi 크롤링을 시작합니다 >> ")
    mcpiCrawler.mcpiCrawler(col2)  # mcpi 지수 크롤링
    time_mcpi = time.time()

    print("time track1")
    print("start1, time_musicCow, time_copyrightPrice, time_mcpi")
    print(start1-start, time_musicCow-start1, time_copyrightPrice-time_musicCow, time_mcpi-time_copyrightPrice)

def track2(NewsArtistListCurrent, start):
    start2 = time.time()

    # ======================================================== << Track 2 >> : 기사 크롤링 ==================================================
    print("<< track2 시작 >>")
    print("<< Naver 크롤링 전반을 시작합니다 >> ")
    crawler_naver_news_link.daily_Naver(dateToday, col5, NewsArtistListCurrent)
    time_naverLink_1 = time.time()

    print("<< Naver  본문 크롤링 전반을 시작합니다 >> ")
    crawler_naver_news_text.update_article_info(dateToday, col5, NewsArtistListCurrent)
    time_naverText_1 = time.time()

    print("time track2")
    print("start2, time_naverLink_1, time_naverText_1")
    print(start2-start, time_naverLink_1-start2, time_naverText_1-time_naverLink_1)

def track3(NewsArtistListCurrent, start):
    start3 = time.time()

    # ======================================================== << Track 3 >> : mcpi 크롤링 ==================================================
    print("<< track3 시작 >>")
    print("<< Naver 크롤링 후반을 시작합니다 >> ")
    crawler_naver_news_link.daily_Naver(dateToday, col5, NewsArtistListCurrent)
    time_naverLink_2 = time.time()

    print("<< Naver  본문 크롤링 후반을 시작합니다 >> ")
    crawler_naver_news_text.update_article_info(dateToday, col5, NewsArtistListCurrent)
    time_naverText_2 = time.time()

    print("time track3")
    print("start3, time_naverLink_2, time_naverText_2")
    print(start3-start, time_naverLink_2-start3, time_naverText_2-time_naverLink_2)

def track4(SongNumListCurrent, NewsArtistListCurrent, start):
    start4 = time.time()

    # ======================================================== << Track 4 >> : 신곡 크롤링 ==================================================
    print("<< track4 시작 >>")

    NewsArtistListNew = list(set(artist_for_nlp.list_artist_query) - set(NewsArtistListCurrent))
    NewsArtistListNew.remove(np.nan)

    # 4-1. newNaver 크롤링
    print("<< 신곡 Naver 크롤링을 시작합니다 >> ")
    crawler_naver_news_link.daily_Naver(dateToday, col5, NewsArtistListNew)
    time_newNaverLink = time.time()
    print("<< 신곡 Naver 본문 크롤링을 시작합니다 >> ")
    crawler_naver_news_text.update_article_info(dateToday, col5, NewsArtistListNew)
    time_newNaverText = time.time()

    # 4-2. newSong 크롤링
    print("<< 신곡 크롤링을 시작합니다 >>")
    newSongList = musicCowCrawler.songCrawlerNew(col1, SongNumListCurrent)
    print(newSongList)
    newArtistList = songSeparator.SongSeparator(col1)
    print(newArtistList)
    time_newSong = time.time()

    # 4-3. musicInfo 크롤링
    print("<< 곡 information 크롤링을 시작합니다 >> ")
    musicInfoCrawler.musicInfoCrawler(col1, col4)
    time_musicInfo = time.time()

    # 4-4. copyrightPrice 크롤링
    print("<< 저작권료 크롤링을 시작합니다 >> ")
    copyrightCrawler.copyrightCrawler(col3, dateToday, newSongList)
    time_copyrightPrice = time.time()

    print("time track4")
    print("start4, time_newNaverLink, time_newNaverText, time_newSong, time_musicInfo, time_copyrightPrice")
    print(start4-start, time_newNaverLink-start4, time_newNaverText-time_newNaverLink, time_newSong-time_newNaverText,
          time_musicInfo-time_newSong, time_copyrightPrice-time_musicInfo)


def init():
    start = time.time()
    print("{0} 크롤링 시작합니다".format(dateToday.strftime('%Y-%m-%d')))

    artist_for_nlp

    # === 크롤링 ===

    SongNumListCurrent = list(col1.find({}, {'num': {"$slice": [1, 1]}}))
    NewsArtistListCurrent = list(col5.find({}).distinct("artist"))
    NewsArtistListFirst = NewsArtistListCurrent[:len(NewsArtistListCurrent) // 2]
    NewsArtistListSecond = NewsArtistListCurrent[len(NewsArtistListCurrent) // 2:]

    with open("storage/check_new/newArtistList.txt", 'w') as f:
        pass
    with open("storage/check_new/newSongList.txt", 'w') as f:
        pass

    pl = Pool(4)

    pl.apply_async(track1, (SongNumListCurrent, start,))
    pl.apply_async(track2, (NewsArtistListFirst, start,))
    pl.apply_async(track3, (NewsArtistListSecond, start, ))
    pl.apply_async(track4, (SongNumListCurrent, NewsArtistListCurrent, start, ))

    pl.start()
    pl.join()


schedule.every().day.at("00:01").do(init)

while True:
    schedule.run_pending()
    time.sleep(1)
