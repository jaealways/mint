#!/usr/bin/env python
# coding: utf-8

# In[2]:


# market.xlsx 불러오기

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
song_title = []

for cell in col1[1:]:
    song_title.append(cell.value)

    
# 가수이름 column 생성
col2 = data['B']
song_artist = []

for cell in col2[1:]:
    song_artist.append(cell.value)
    
    

# 노래제목, 가수이름 한 리스트로 합치기
matching = []
for i in range(731):
    matching.append([song_title[i], song_artist[i]])



# Dataframe으로 만들기
df = pd.DataFrame(matching)
df.columns = ['노래제목', '가수']

print(df)


# In[ ]:


#=============== 노래제목 영어 / 한국어 나누기 알고리즘 =============== #
# == 가수이름 나누기 알고리즘과 동일# 
# bts, 홍길동, 임꺽정, (honggildong) 이런식으로 써져있으니
# main part / 괄호 part 로 1차 분리
# 각 part 안에서 영어 / 한국어 2차 분리
# 총 결과물 :  main_eng, main_kor, split_eng, split_kor  


# In[3]:


#========================================== 1차분리 : main 노래제목 ============================================#
song_title_main=[]
for titles in df['노래제목']:
    omitetc=re.sub(r'\([^)]*\)', '', titles)
    song_title_main.append(omitetc)


# In[5]:


# main - 2차 분리 : main 영어 노래제목
song_title_main_english=[]
for title_main_eng in song_title_main:
    if title_main_eng[0].encode().isalpha():
        song_title_main_english.append(title_main_eng)
    else:
        song_title_main_english.append("")

            


# In[6]:


song_title_main_english


# In[7]:


# main 2차 분리 : main 한국어 노래제목
song_title_main_korean=[]
for title_main_kor in song_title_main:
    if title_main_kor[0].encode().isalpha():
        song_title_main_korean.append("")
    else:
        song_title_main_korean.append(title_main_kor)


# In[8]:


song_title_main_korean


# In[9]:


#==================================== 1차 분리 : 괄호(split) 노래제목==========================================#
import re

song_title_split=[]
song_title_featuring=[]
for titles in df['노래제목']:
    item = re.findall('\(([^)]+)', titles)

    song_title_split.append(item)


# In[14]:


song_titles_split=[]
song_titles_featuring=[]
for titles in df['노래제목']:
    item = re.findall('\(([^)]+)', titles)

    song_titles_split.append(item)


# In[10]:


song_title_split


# In[26]:


# 2차 분리 :  괄호 내 노래제목 영어 / 한국어 분리 ==================================#
song_title_split_english=[]
song_title_split_korean=[]
song_title_split_featuring=[]
for title_split in song_title_split:
    song_title_split_english_low=[]
    song_title_split_korean_low=[]
    song_title_split_featuring_low=[]
    for i in title_split:
        if i[0].encode().isalpha():
            if 'Feat.' in i:
                del title_split[title_split.index(i)]
                song_title_split_featuring_low.append(i)
            elif 'feat.' in i:
                del title_split[title_split.index(i)]
                song_title_split_featuring_low.append(i)
            elif 'Prod.' in i:
                del title_split[title_split.index(i)]
                song_title_split_featuring_low.append(i)
            elif 'prod.' in i:
                del title_split[title_split.index(i)]
                song_title_split_featuring_low.append(i)
            elif 'PROD.' in i:
                del title_split[title_split.index(i)]
                song_title_split_featuring_low.append(i)
            song_title_split_english_low.append(i)
        else:
            song_title_split_korean_low.append(i)
    song_title_split_english.append(song_title_split_english_low)
    song_title_split_korean.append(song_title_split_korean_low)
    song_title_split_featuring.append(song_title_split_featuring_low)
            
        


# In[27]:


song_title_split_english  # 괄호 내 영어 노래제목 - feat, prod 제거


# In[28]:


song_title_split_korean  # 괄호 내 한국어 노래제목


# In[29]:


song_title_split_featuring  # 괄호 내 feat


# In[33]:


# 영어 노래제목, 한국어 노래제목 한 리스트로 합치기
matching_artist_korean_english = []
for i in range(731):
    matching_artist_korean_english.append([song_artist_split_korean[i], song_artist_main_korean[i], song_artist_split_english[i], song_artist_main_english[i]])



# Dataframe으로 만들기
df_song_artist_eng_kor = pd.DataFrame(matching_artist_korean_english)
df_song_artist_eng_kor.columns=['괄호내 가수_kor', '메인가수_kor', '괄호내 가수_eng', '메인가수_eng']
print(df_song_artist_eng_kor)


# In[48]:


song_artist_feat=[]
for split_artist_eng in song_artist_split_english:
    for i in split_artist_eng:
        if 'Feat.' in i:
            feat_index = i.find("Feat")
            song_artist_feat.append(i)
        elif 'Prod.' in i:
            feat_index = i.find("Prod")
            song_artist_feat.append(i)
        elif 'FEAT.' in i:
            feat_index = i.find("FEAT")
            song_artist_feat.append(i)
        elif 'feat.' in i:
            feat_index = i.find("feat")
            song_artist_feat.append(i)
        elif 'PROD.' in i:
            feat_index = i.find("PROD")
            song_artist_feat.append(i)


# In[49]:


song_artist_feat


# In[44]:


df_song_artist_eng_kor[['괄호내 가수_eng']]


# In[43]:


song_artist_feat


# In[45]:


df_song_artist_eng_kor


# In[32]:


df_song_artist_eng_kor.to_excel('가수이름분리_5.xlsx')


# In[75]:


## DataFrame 만들기
df = pd.DataFrame({"가수이름분리 ": matching_artist_korean_english})
 
## XlsxWriter 엔진으로 Pandas writer 객체 만들기
writer = pd.ExcelWriter("가수이름분리.xlsx", engine="xlsxwriter")
 
## DataFrame을 xlsx에 쓰기
df.to_excel(writer, sheet_name='Sheet1', startrow=1, header=False)
 
## Pandas writer 객체에서 xlsxwriter 객체 가져오기
workbook = writer.book
worksheet = writer.sheets['Sheet1']
 

writer.close()

