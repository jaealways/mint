# AWS DynamoDB 연동 전까지 임시 크롤러

import requests
import re
from bs4 import BeautifulSoup
import openpyxl
from openpyxl import load_workbook
import pandas as pd

# ================= 노래제목 - 가수이름 짝지어 dataframe 만들기 =================== #
# market.xlsx 불러오기
market_sheet = load_workbook("market.xlsx")
data = market_sheet.active


# 노래 제목 column 생성
col1 = data['A']
title = []

for cell in col1[1:]:
    title.append(cell.value)

song_title=[]
for i in title:   # 공백(space) 없애기
    title_omitspace = i.replace(' ','')
    song_title.append(title_omitspace)



# 가수이름 column 생성
col2 = data['B']
artist = []

for cell in col2[1:]:
    artist.append(cell.value)

song_artist=[]
for i in artist:   # 공백(space) 없애기
    artist_omitspace = i.replace(' ','')
    song_artist.append(artist_omitspace)





# 노래제목에 맞는 가수이름 matching 하여 한 리스크로 합치기
matching = []
for i in range(731):
    matching.append([song_artist[i] +' '+ song_title[i]])

# matching 에 '가수이름v노래제목' 리스트 731개 들어있음




# ================================ 트위터 크롤링 ============================================ #
# df 를 대입하여 크롤링
# 빈칸이 %20 으로 표현됨. '(작은따옴표) 는 %27


# for songs in matching:
#     page = "https://twitter.com/search?q={0}&src=typed_query".format(songs)
#     url = requests.get(page)
#     html = url.text
#     soup = BeautifulSoup(html, 'html.parser')
#
#     tweet_num = str(soup.select('div.css-1dbjc4n.r-j7yic.r-qklmqi.r-ladg311.r1ny4131'))
#     tweet_num = re.sub('<.+?>', '', song_title, 0).strip()


# ================================ 유튜브 크롤링 ============================================ #






# ================================ 구글 크롤링 ============================================ #
# df 를 대입하여 크롤링