# 트위터_크롤링.py - YW 수정 04 / 10

import requests
import re
from bs4 import BeautifulSoup
from openpyxl import load_workbook
import pandas as pd


# ================= 노래제목 - 가수이름 짝지어 dataframe 만들기 =================== #
# market.xlsx 불러오기
market_sheet = load_workbook("C:/Users/ninay/Desktop/pythonworkspace/kuggle_project/market.xlsx") 
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


for songs in matching:
    page = "https://twitter.com/search?q={0}&src=typed_query".format(songs)
    url = requests.get(page)
    html = url.text
    soup = BeautifulSoup(html, 'html.parser')

    tweet_num = str(soup.select('div.css-1dbjc4n.r-j7yic.r-qklmqi.r-ladg311.r1ny4131'))
    tweet_num = re.sub('<.+?>', '', song_title, 0).strip()



################ 04 / 09 코딩 일지 ################
## 문제 1 : 가수이름과 노래제목 검색url에 입력시 표현 난해함 문제 

# 가수이름_노래제목 이런식으로 트위터 검색창에 검색하면 ex) 이승기 연애시대 
# url에는 이승기%20연애시대 ==> 이런식으로 띄어쓰기가 %20 으로 표시됨.

# 그런데, matching 리스트에서 추출하여 '라디 (Ra. D)', "I'm in love" 이런식으로 검색하면
# %27라디%20(Ra.%20D)%27%2C%20"I%27m%20in%20love" 이런식으로 url에 표시됨.

# 따라서 matching 리스트 데이터 예쁘게 다듬는 작업 필요함 ex) '라디 (Ra. D)', "I'm in love"  ==> 라디(Ra.D) I'minlove 



## 문제 2 : 크롤링 할 때 카테고리 수 세는 법 모르겠음
# 한 section(게시글) 당 할당된 동일한 class 명은 div.css-1dbjc4n.r-j7yic.r-qklmqi.r-ladg311.r1ny4131 임 
# 근데 이 class명을 가진 걸 어떻게 세는 지 모르겠습니다 ... 







################ 04 / 10 코딩 일지 ################
## 해결 1 :  04/09 문제 1 대체로 해결
# 불필요한 공백(space) 없앰. 가수이름v제목 => 이런식으로 한 문자열로 만듦
#  ex) '라디 (Ra. D)', "I'm in love"  ==> '라디(Ra.D) I'minlove'     ==>    가수이름과 노래제목 사이에만 공백 존재하도록 바꿈.
#
# 변화된 점 : url 의 간소화
# %27라디(Ra.D)%20I%27minlove%27 이런식으로 간소화됨
#
## 문제 1 : '(작은따옴표) 는 %27로 표현됨
# 노래마다 무질서하게 있는 ' 를 어떻게 발견해서 %27로 바꿀 것인지 + 문자열 전체를 감싸는 ''를 제거할 수 있는 방안


## 문제 2 : 검색어의 정확도, 방법론
# MC몽 사랑범벅(Feat.챈슬러ofthechannels) ==> 예를들면, 
# 검색어 : MC몽 사랑범벅(Feat.챈슬러ofthechannels)      ==> 검색결과 x
# 검색어 : MC몽 사랑범벅                                ==> 검색결과 o
#
# 결론 : 제목뒤에 붙는 괄호 [ex) feat.아무개, prod.아무개] 는 지워도 큰 정확도 차이가 없으므로, 지우는게 나음. 근데 문제는 어떻게 지우죠 ??


## 문제 3 : 여전히 04/09 문제 2 해결 못함 ... 


