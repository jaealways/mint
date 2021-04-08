# 트위터 트윗 수 크롤링 - 정예원

import requests
import re
from bs4 import BeautifulSoup
from openpyxl import load_workbook
import pandas as pd
from selenium import webdriver


# ================= 노래제목 - 가수이름 짝지어 dataframe 만들기 =================== #
# market.xlsx 불러오기
market_sheet = load_workbook("C:/Users/ninay/Desktop/pythonworkspace/kuggle_project/market.xlsx") 
data = market_sheet.active

# 노래 제목 column 생성
col1 = data['A']
song_title = []

for cell in col1[1:]:
    song_title.append(cell.value)

    
# 가수이름 column 생성
col2 = data['B']
song_artist = []

for cell in col2[1:]:
    song_artist.append(cell.value)
    
    

# 노래제목에 맞는 가수이름 matching 하여 한 리스크로 합치기
matching = []
for i in range(731):
    matching.append([song_artist[i], song_title[i]])



# Dataframe으로 만들기
df = pd.DataFrame(matching, columns={'가수', '노래제목'})
print(df)



# ================================ 트위터 크롤링 ============================================ #
# df 를 대입하여 크롤링
# 빈칸이 %20 으로 표현됨


for songs in matching:
    page = "https://twitter.com/search?q={0}&src=typed_query".format(songs)
    url = requests.get(page)
    html = url.text
    soup = BeautifulSoup(html, 'html.parser')

    tweet_num = str(soup.select('div.css-1dbjc4n.r-j7yic.r-qklmqi.r-ladg311.r1ny4131'))
    tweet_num = re.sub('<.+?>', '', song_title, 0).strip()




############### 문제 1 : 가수이름과 노래제목 검색url에 입력시 표현 난해함 문제 #################

# 가수이름_노래제목 이런식으로 트위터 검색창에 검색하면 ex) 이승기 연애시대 
# url에는 이승기%20연애시대 ==> 이런식으로 띄어쓰기가 %20 으로 표시됨.

# 그런데, matching 리스트에서 추출하여 '라디 (Ra. D)', "I'm in love" 이런식으로 검색하면
# %27라디%20(Ra.%20D)%27%2C%20"I%27m%20in%20love" 이런식으로 url에 표시됨.

# 따라서 matching 리스트 데이터 예쁘게 다듬는 작업 필요함 ex) '라디 (Ra. D)', "I'm in love"  ==> 라디(Ra.D) I'minlove 



################ 문제 2 : 크롤링 할 때 카테고리 수 세는 법 모르겠음 ################3
# 한 section(게시글) 당 할당된 동일한 class 명은 div.css-1dbjc4n.r-j7yic.r-qklmqi.r-ladg311.r1ny4131 임 
# 근데 이 class명을 가진 걸 어떻게 세는 지 모르겠습니다 ... 
