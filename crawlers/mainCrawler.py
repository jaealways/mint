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
import musicCowCrawler
import songSeparator
import copyrightCrawler
import musicInfoCrawler
import mcpiCrawler

from pymongo import MongoClient
import os
from multiprocessing import Process

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

# ====================================== << Track 1 >> : 현재 musicCowData 디비에 있는 곡들 기준 크롤링 =========================================
# 1. 현재 musicCowData 디비에 있는 곡들 / 가수들
# 1-1. DB에 있는 곡들 대상으로 뮤직카우 데이터 크롤링
print("<< 뮤직카우 데이터 크롤링을 시작합니다 >>")
musicCowCrawler.songCrawler(col1)      # 뮤직카우 디비에 있는 기존 곡들 크롤링

# 1-2. musicInfo 크롤링
print("<< 곡 information 크롤링을 시작합니다 >> ")
musicInfoCrawler.musicInfoCrawler(col1, col4)

# 1-3. copyrightPrice 크롤링
print("<< 저작권료 크롤링을 시작합니다 >> ")
copyrightCrawler.copyrightCrawler(col1, col3)

# 1-4. Naver 크롤링
#print("<< Naver 크롤링을 시작합니다 >> ")



# ======================================================== << Track 2 >> : 신곡 크롤링 ==================================================

print("<< 신곡 크롤링을 시작합니다 >>")
newSongList = musicCowCrawler.songCrawlerNew(col1)
print(newSongList)

# ======================================================== << Track 3 >> : mcpi 크롤링 ==================================================
print("<< mcpi 크롤링을 시작합니다 >> ")
mcpiCrawler.mcpiCrawler(col2)         # mcpi 지수 크롤링


