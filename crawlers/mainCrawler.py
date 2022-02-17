# << 메인 크롤링 코드 >>
# 작성자 : 정예원

# 코드 개선사항
# 크롤링 1 종료후
# 크롤링 2 크롤링 3 크롤링 4 thread 나눠서 시간 단축

# modules
import musicCowCrawler
import copyrightPriceCrawler

from pymongo import MongoClient


 # == 몽고디비 ==
client = MongoClient('localhost', 27017)
db1 = client.music_cow
db2 = client.article

# music cow
col1 = db1.musicCowData
col2 = db1.mcpi
col3 = db1.copyright_price

# article
col4 = db2.articleInfo

musicCowSongNumListCurrent = col1.find({}, {'num': {"$slice": [1, 1]}})   # 현재까지 수집한 뮤직카우 콜렉션에 있는 곡 리스트
musicCowArtistListCurrent = col1.find({}, {'song_artist': {"$slice": [1, 1]}})  # 현재까지 수집한 뮤직카우 콜렉션에 있는 가수 리스트


# == 크롤링 ==

# 1. 뮤직카우 데이터 크롤링
# print("<< 뮤직카우 데이터 크롤링을 시작합니다 >>")
# musicCowData = musicCowCrawler.MusicCowCrawler(col1, col3, musicCowSongNumListCurrent, musicCowArtistListCurrent)
# musicCowData.songCrawler()      # 뮤직카우 디비에 있는 기존 곡들 크롤링
# musicCowData.songCrawlerNew()   # 뮤직카우에 새로 등록된 곡들 크롤링
# print("뮤직카우에 새로 등록된 가수 명단입니다")
# print(musicCowData.newArtistList)

# input("")

# # 2. 저작권료 크롤링
print("<< 저작권료 크롤링을 시작합니다 >> ")
copyrightPriceData = copyrightPriceCrawler.CopyrightPriceCrawler(col1, col3)
copyrightPriceData.copyrightPrice()
