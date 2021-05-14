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
df = pd.DataFrame(matching, columns={'가수', '노래제목'})
print(df)


# In[ ]:


#=============== 가수 이름 영어 / 한국어 나누기 알고리즘 =============== #
# bts, 홍길동, 임꺽정, (honggildong) 이런식으로 써져있으니
# main part / 괄호 part 로 1차 분리
# 각 part 안에서 영어 / 한국어 2차 분리
# 총 결과물 :  main_eng, main_kor, split_eng, split_kor  


# In[3]:


#========================================== 1차분리 : main 가수 이름 ============================================#
song_artist_main=[]
for names in df['가수']:
    omitetc=re.sub(r'\([^)]*\)', '', names)
    song_artist_main.append(omitetc)


# In[5]:


# main - 2차 분리 : main 가수 영어 이름
song_artist_main_english=[]
for artist_name in song_artist_main:
    if artist_name[0].encode().isalpha():
        song_artist_main_english.append(artist_name)
    else:
        song_artist_main_english.append("")

            


# In[6]:


song_artist_main_english


# In[7]:


# main 2차 분리 : main 가수 한국어 이름
song_artist_main_korean=[]
for artist_name in song_artist_main:
    if artist_name[0].encode().isalpha():
        song_artist_main_korean.append("")
    else:
        song_artist_main_korean.append(artist_name)


# In[8]:


song_artist_main_korean


# In[9]:


#==================================== 1차 분리 : 괄호(split) 가수 이름==========================================#
import re

song_artist_split=[]
song_arits_featuring=[]
for titles in df['가수']:
    item = re.findall('\(([^)]+)', titles)

    song_artist_split.append(item)


# In[4]:


song_titles_split=[]
song_titles_featuring=[]
for titles in df['노래제목']:
    item = re.findall('\(([^)]+)', titles)

    song_titles_split.append(item)


# In[10]:


song_artist_split


# In[11]:


# 2차 분리 :  괄호 내 가수이름 영어 / 한국어 분리 ==================================#
song_artist_split_english=[]
song_artist_split_korean=[]
for artist_name in song_artist_split:
    song_artist_split_english_low=[]
    song_artist_split_korean_low=[]
    for i in artist_name:
        if i[0].encode().isalpha():
            song_artist_split_english_low.append(i)
        else:
            song_artist_split_korean_low.append(i)
    song_artist_split_english.append(song_artist_split_english_low)
    song_artist_split_korean.append(song_artist_split_korean_low)
            
        


# In[12]:


song_artist_split_english  # 괄호 내 영어 가수이름


# In[13]:


song_artist_split_korean  # 괄호 내 한국어 가수 이름


# In[14]:


# 영어 가수이름, 한국어 가수이름 한 리스트로 합치기
matching_artist_korean_english = []
for i in range(731):
    matching_artist_korean_english.append([song_artist_split_korean[i], song_artist_main_korean[i], song_artist_split_english[i], song_artist_main_english[i]])



# Dataframe으로 만들기
df_song_artist_eng_kor = pd.DataFrame(matching_artist_korean_english, columns={'메인 가수_eng', '괄호내 가수_kor', '메인 가수_kor', '괄호내 가수_eng'})
print(df_song_artist_eng_kor)


# In[15]:


df_song_artist_eng_kor


# In[16]:


df_song_artist_eng_kor.to_excel('가수이름분리_2.xlsx')


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

