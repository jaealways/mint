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



#========================================== 1차분리(nonround / round) : nonround추출 - main 노래제목 ============================================#
song_title_main=[]            # non round part : song_title_main
for titles in df['노래제목']:
    omitetc=re.sub(r'\([^)]*\)', '', titles)
    song_title_main.append(omitetc)

# song_title_main - 2차 분리 : main 영어 노래제목
song_title_main_english=[]  # 괄호 밖 eng 노래제목  ****************************
for title_main_eng in song_title_main:
    if title_main_eng[0].encode().isalpha():
        song_title_main_english.append(title_main_eng)
    else:
        song_title_main_english.append("")

# main 2차 분리 : main 한국어 노래제목
song_title_main_korean=[]   # 괄호 밖 kor 노래제목 ******************************
for title_main_kor in song_title_main:
    if title_main_kor[0].encode().isalpha():
        song_title_main_korean.append("")
    else:
        song_title_main_korean.append(title_main_kor)



#==================================== 1차분리(nonround / round) : nonround추출 - 괄호내 노래제목==========================================#
song_title_sub=[]             # round part : song_title_sub
song_title_featuring=[]
for titles in df['노래제목']:
    item = re.findall('\(([^)]+)', titles)

    song_title_sub.append(item)

# 2차 분리 :  괄호 내 노래제목 영어 / 한국어 분리
song_title_sub_english = []      # 괄호 내 영어제목
song_title_sub_korean = []       # 괄호 내 한글제목
song_title_sub_featuring = []    # 괄호 내 피처링
for title_sub in song_title_sub:
    song_title_sub_english_low = []
    song_title_sub_korean_low = []
    song_title_sub_featuring_low = []
    for i in title_sub:
        if i[0].encode().isalpha():                          # 영어인지
            if 'Feat.' in i:                                 # 영어인데 feat, prod 등 들어가 있으면 featuring.
                del title_sub[title_sub.index(i)]
                song_title_sub_featuring_low.append(i)
            elif 'feat.' in i:
                del title_sub[title_sub.index(i)]
                song_title_sub_featuring_low.append(i)
            elif 'Prod.' in i:
                del title_sub[title_sub.index(i)]
                song_title_sub_featuring_low.append(i)
            elif 'prod.' in i:
                del title_sub[title_sub.index(i)]
                song_title_sub_featuring_low.append(i)
            elif 'PROD.' in i:
                del title_sub[title_sub.index(i)]
                song_title_sub_featuring_low.append(i)
            song_title_sub_english_low.append(i)
        else:                                               # 영어가 아니면 한국어로 분류
            song_title_sub_korean_low.append(i)
    song_title_sub_english.append(song_title_sub_english_low)
    song_title_sub_korean.append(song_title_sub_korean_low)
    song_title_sub_featuring.append(song_title_sub_featuring_low)





# feat 제외한 괄호 내 영어/한국어 제목 추출
song_title_sub_english=[]     # 괄호 내 feat 제외 영어 제목******************
song_title_sub_korean=[]      # 괄호 내 feat 제외 한국어 제목******************
for title_sub in song_title_sub:
    song_title_sub_english_low=[]
    song_title_sub_korean_low=[]
    song_title_sub_featuring_low=[]
    for i in title_sub:
        if i[0].encode().isalpha():
            song_title_sub_english_low.append(i)
        else:
            song_title_sub_korean_low.append(i)
    song_title_sub_english.append(song_title_sub_english_low)
    song_title_sub_korean.append(song_title_sub_korean_low)

for i in song_title_sub_featuring:
    print(i)
# 피처링 가수에서 feat. prod 등 지우기
song_artist_feat_eng=[]     # feat eng******************
song_artist_feat_kor=[]     # feat kor********************
for feat_singer in song_title_sub_featuring:
    if str(feat_singer).find('Feat.') != -1:
        omitfeat = str(feat_singer).replace("Feat.", "")
        if omitfeat[2].encode().isalpha():
            song_artist_feat_eng.append(omitfeat)
        else:
            song_artist_feat_kor.append(omitfeat)
    elif str(feat_singer).find('Prod.') != -1:
        omitfeat = str(feat_singer).replace("Prod.", "")
        if omitfeat[2].encode().isalpha():
            song_artist_feat_eng.append(omitfeat)
        else:
            song_artist_feat_kor.append(omitfeat)
    elif str(feat_singer).find('prod.') != -1:
        omitfeat = str(feat_singer).replace("prod.", "")
        if omitfeat[2].encode().isalpha():
            song_artist_feat_eng.append(omitfeat)
        else:
            song_artist_feat_kor.append(omitfeat)
    elif str(feat_singer).find('feat.') != -1:
        omitfeat = str(feat_singer).replace("feat.", "")
        if omitfeat[2].encode().isalpha():
            song_artist_feat_eng.append(omitfeat)
        else:
            song_artist_feat_kor.append(omitfeat)
    elif str(feat_singer).find('PROD.') != -1:
        omitfeat = str(feat_singer).replace("PROD.", "")
        if omitfeat[2].encode().isalpha():
            song_artist_feat_eng.append(omitfeat)
        else:
            song_artist_feat_kor.append(omitfeat)
    else:
        song_artist_feat_eng.append(str(feat_singer))
        song_artist_feat_kor.append(str(feat_singer))

print(len(song_artist_feat_eng))
print(len(song_artist_feat_kor))
# 피처링에서 영어 / 한국어 나누기

#print(song_artist_feat_eng)
#print(song_artist_feat_kor)

#title_split = []
#for i in range(731):
#    title_split.append([song_title_sub_korean[i], song_title_main_korean[i], song_title_sub_english[i], song_title_main_english[i]])

# 제목 Dataframe으로 만들기
#df_song_title_eng_kor = pd.DataFrame(title_split)
#df_song_title_eng_kor.columns=['괄호내 제목_kor', '메인제목_kor', '괄호내 제목_eng', '메인제목_eng']
#print(df_song_title_eng_kor)






#=================================================== 가수이름 나누기 =================================================
#========================================== 1차분리(nonround / round) : nonround추출 - 괄호 밖 메인가수 ============================================#
song_artist_main=[]    # 괄호 밖 메인 가수
for names in df['가수']:
    omitetc=re.sub(r'\([^)]*\)', '', names)
    song_artist_main.append(omitetc)


# song_artist_main - 2차 분리 : main 영어 가수이름
song_artist_main_english=[]
for artist_name in song_artist_main:
    if artist_name[0].encode().isalpha():
        song_artist_main_english.append(artist_name)
    else:
        song_artist_main_english.append("")


# song_artist_main - 2차 분리 : main 한국어 가수이름
song_artist_main_korean=[]
for artist_name in song_artist_main:
    if artist_name[0].encode().isalpha():
        song_artist_main_korean.append("")
    else:
        song_artist_main_korean.append(artist_name)



#========================================== 1차분리(nonround / round) : round추출 - 괄호 안 서브가수 ==================================================
song_artist_sub=[]
song_artist_featuring=[]
for titles in df['가수']:
    item = re.findall('\(([^)]+)', titles)

    song_artist_sub.append(item)

# song_artist_sub - 2차 분리 : sub 영어 / 한국어 가수이름
song_artist_sub_english=[]
song_artist_sub_korean=[]
for artist_name in song_artist_sub:
    song_artist_sub_english_low=[]
    song_artist_sub_korean_low=[]
    for i in artist_name:
        if i[0].encode().isalpha():
            song_artist_sub_english_low.append(i)
        else:
            song_artist_sub_korean_low.append(i)
    song_artist_sub_english.append(song_artist_sub_english_low)
    song_artist_sub_korean.append(song_artist_sub_korean_low)



# not featuring - english
song_artist_nonfeat_eng = []  # 괄호밖 영어 + 괄호 안 영어 가수이름 합치기  ==> 가수이름은 괄호 안과 밖이 모두 사실은 main 가수이므로
# ex) apink(에이핑크) / 라디(Ra.D) ==>  song_artist_main_eng = [[],[],'apink',[]], song_artist_sub_end = [[],Ra.D,[],[]]   ==> song_artist_english = [[],Ra.D,apink,[]]
song_artist_nonfeat_eng = list(zip(song_artist_main_english,song_artist_sub_english))  #********************


# not featuring - korean
song_artist_nonfeat_kor = []
song_artist_nonfeat_kor = list(zip(song_artist_main_korean,song_artist_sub_korean))    #*******************


# 영어 가수이름, 한국어 가수이름 한 리스트로 합치기
#artist_split = []
#for i in range(731):
#    artist_split.append([song_artist_nonfeat_eng[i], song_artist_nonfeat_kor[i], song_artist_sub_english[i], song_artist_main_english[i]])



# Dataframe으로 만들기
#df_song_artist_eng_kor = pd.DataFrame(artist_split)
#df_song_artist_eng_kor.columns=['메인가수_eng', '메인가수_kor', '괄호내 가수_eng', '메인가수_eng']
#print(df_song_artist_eng_kor)



# 영어 가수이름, 한국어 가수이름 한 리스트로 합치기
all_title= []
for i in range(731):
    all_title.append([song_title_main_english[i],song_title_main_korean[i],song_title_sub_english[i],song_title_sub_korean[i]])

all_artist=[]
for i in range(731):
    all_artist.append([song_artist_nonfeat_eng[i], song_artist_nonfeat_kor[i]])

#,'메인가수_eng','메인가수_kor','서브가수_eng','서브가수_kor'
# Dataframe으로 만들기
df_all_title = pd.DataFrame(all_title)
df_all_title.columns=['메인제목_eng', '메인제목_kor','서브제목_eng','서브제목_kor']
print(df_all_title)

df_all_artist = pd.DataFrame(all_artist)
df_all_artist.columns=['메인가수_eng','메인가수_kor']
print(df_all_artist)

df_all_title_artist = pd.concat([df_all_title, df_all_artist], axis = 1)
print(df_all_title_artist)
