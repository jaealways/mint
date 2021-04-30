import pandas as pd
from pandas import DataFrame
 
import requests
import re
from bs4 import BeautifulSoup
from openpyxl import load_workbook


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




# ================================= 괄호 없애기 ================================== #
song_title_omitfeat=[]
for titles in song_title:
    if 'Feat.' in titles:
        feat_index = titles.find("Feat")
        song_title_omitfeat.append(titles[:feat_index - 1])
    elif 'Prod.' in titles:
        feat_index = titles.find("Prod")
        song_title_omitfeat.append(titles[:feat_index - 1])
    elif 'FEAT.' in titles:
        feat_index = titles.find("FEAT")
        song_title_omitfeat.append(titles[:feat_index - 1])
    elif 'feat.' in titles:
        feat_index = titles.find("feat")
        song_title_omitfeat.append(titles[:feat_index - 1])
    elif 'PROD.' in titles:
        feat_index = titles.find("PROD")
        song_title_omitfeat.append(titles[:feat_index - 1])
    else:
        song_title_omitfeat.append(titles)
    


## DataFrame 만들기
df = DataFrame({"노래제목(괄호없앤)": song_title_omitfeat })
 
## XlsxWriter 엔진으로 Pandas writer 객체 만들기
writer = pd.ExcelWriter("괄호 없앤것.xlsx", engine="xlsxwriter")
 
## DataFrame을 xlsx에 쓰기
df.to_excel(writer, sheet_name='Sheet1', startrow=1, header=False)
 
## Pandas writer 객체에서 xlsxwriter 객체 가져오기
workbook = writer.book
worksheet = writer.sheets['Sheet1']
 

 
### Pandas writer 객체 닫기
writer.close()
