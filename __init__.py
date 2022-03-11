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


from crawlers import musicCowCrawler, songSeparator, copyrightCrawler, musicInfoCrawler, mcpiCrawler


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
col5 = db2.articleInfo

# === 크롤링 ===

def track1(SongNumListCurrent):
    # ====================================== << Track 1 >> : 현재 musicCowData 디비에 있는 곡들 기준 크롤링 =========================================
    # 1. 현재 musicCowData 디비에 있는 곡들 / 가수들
    # 1-1. DB에 있는 곡들 대상으로 뮤직카우 데이터 크롤링
    print("<< track1 시작 >>")
    print("<< 뮤직카우 데이터 크롤링을 시작합니다 >>")
    musicCowCrawler.songCrawler(col1, SongNumListCurrent)      # 뮤직카우 디비에 있는 기존 곡들 크롤링

    # 1-2. copyrightPrice 크롤링
    print("<< 저작권료 크롤링을 시작합니다 >> ")
    copyrightCrawler.copyrightCrawler(col3, dateToday, SongNumListCurrent)


def track2(SongNumListCurrent):
    # ======================================================== << Track 2 >> : 기사 크롤링 ==================================================
    print("<< track2 시작 >>")
    print("<< Naver 크롤링을 시작합니다 >> ")


def track3(SongNumListCurrent):
    # ======================================================== << Track 3 >> : 신곡 크롤링 ==================================================

    # 3-1. newSong 크롤링
    print("<< track3 시작 >>")
    print("<< 신곡 크롤링을 시작합니다 >>")
    newSongList = musicCowCrawler.songCrawlerNew(col1, SongNumListCurrent)
    print(newSongList)
    newArtistList = songSeparator.SongSeparator(col1, newSongList)
    print(newArtistList)

    # 3-2. musicInfo 크롤링
    print("<< 곡 information 크롤링을 시작합니다 >> ")
    musicInfoCrawler.musicInfoCrawler(col1, col4)

    # 3-3. copyrightPrice 크롤링
    print("<< 저작권료 크롤링을 시작합니다 >> ")
    copyrightCrawler.copyrightCrawler(col3, dateToday, newSongList)

def track4(SongNumListCurrent):
    # ======================================================== << Track 4 >> : mcpi 크롤링 ==================================================
    print("<< track4 시작 >>")
    print("<< mcpi 크롤링을 시작합니다 >> ")
    mcpiCrawler.mcpiCrawler(col2)         # mcpi 지수 크롤링


# schedule.every().day.at("15:23").do(test_function)
# schedule.every().day.at("15:20").do(test_function2)
# schedule.every().day.at("15:24").do(exit)
#
#
# while True:
#     schedule.run_pending()
#     time.sleep(1)

if __name__ == '__main__':
    dateToday = datetime.datetime.today()
    print("{0} 크롤링 시작합니다".format(dateToday.strftime('%Y-%m-%d')))

    SongNumListCurrent = list(col1.find({}, {'num': {"$slice": [1, 1]}}))

    with open("storage/check_new/newArtistList.txt", 'w') as f:
        pass
    with open("storage/check_new/newSongList.txt", 'w') as f:
        pass

    p1 = Process(target=track1(SongNumListCurrent))
    p1.start()
    p2 = Process(target=track2(SongNumListCurrent))
    p2.start()
    p3 = Process(target=track3(SongNumListCurrent))
    p3.start()
    p4 = Process(target=track4(SongNumListCurrent))
    p4.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()
