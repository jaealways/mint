# << 메인 크롤링 코드 >>
# 작성자 : 정예원
#
# [코드 설명]
# 1. 먼저, musicCowData 콜렉션에 미리 저장되어있던 곡들을 대상으로 뮤직카우 데이터를 크롤링합니다 (?분) (이전에 크롤링을 언제 돌렸는지에 따라 상이)
# 2. 그 다음, 뮤직카우 신곡을 감지해 크롤링을 하여 musicCowData 콜렉션에 추가합니다 (10분) 
# 3. 신곡 크롤링까지 마친 musicCowData 에 있는 곡들을 대상으로, 현재 musicInfo 콜렉션에 없는 곡들에 대하여 곡 정보를 musicInfo 에 저장합니다. (5분)
# 
# 코드 수행 시간 : 약 20분 내외
#
# [현재 코드 완료 진행상황]
# 기존곡 크롤링(songCrawler) -> 신곡 감지 크롤링(songCrawlerNew) -> 곡 info 크롤링 (musicInfoCrawler) 순으로 크롤링이 진행됩니다.
#
#
# [아직 못한 것들]
# 가수명/노래제목 split 적용, mcpiCrawler 연결, copyrightCrawler 연결(연결은 됐으나, 시간소요문제와 디비 저장 오류 수정중) , naverCrawler 연결

# modules
import musicCowCrawler
import songSeparator
import copyrightPriceCrawler
import musicInfoCrawler

import multiprocessing
from pymongo import MongoClient


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

musicCowSongNumListCurrent = col1.find({}, {'num': {"$slice": [1, 1]}})   # 현재까지 수집한 뮤직카우 콜렉션에 있는 곡 리스트
musicCowArtistListCurrent = col1.find({}, {'song_artist': {"$slice": [1, 1]}})  # 현재까지 수집한 뮤직카우 콜렉션에 있는 가수 리스트


# == 크롤링 ==

# 1. 뮤직카우 데이터 크롤링
print("<< 뮤직카우 데이터 크롤링을 시작합니다 >>")
musicCowData = musicCowCrawler.MusicCowCrawler(col1, musicCowSongNumListCurrent, musicCowArtistListCurrent)
musicCowData.songCrawler()      # 뮤직카우 디비에 있는 기존 곡들 크롤링
musicCowData.songCrawlerNew()   # 뮤직카우에 새로 등록된 곡들 크롤링
# print("< DB에 새로 등록된 가수명과 노래제목의 separate 를 시작합니다 >")
# songSeparated = songSeparator.SongSeparator(col1, musicCowData.newSongNumList)
# songSeparated.read_db()
# print("뮤직카우에 새로 등록된 가수 명단입니다")
# print(songSeparated.newArtistList)


# # 2. 저작권료 크롤링 (오류 수정중)
#print("<< 저작권료 크롤링을 시작합니다 >> ")
# copyrightPriceData = copyrightPriceCrawler.CopyrightPriceCrawler(col1, col3)
# pool = multiprocessing.Pool(processes=4)
# pool.map(copyrightPriceData.copyrightPrice(), copyrightPriceData.musicCowSongNumListCurrent)
# pool.close()
# pool.join()



# # 3. 곡 information 크롤링
print("<< 곡 information 크롤링을 시작합니다 >> ")
musicInfoData = musicInfoCrawler.MusicInfoCrawler(col1, col4)
musicInfoData.identify_link()


# # 4. mcpi 크롤링
# print("<< mcpi 크롤링을 시작합니다 >> ")
#
#
# # 5. article + article_text 크롤링
# print("<< article 크롤링을 시작합니다 >> ")
# print("<< article_text 크롤링을 시작합니다 >> ")
#
