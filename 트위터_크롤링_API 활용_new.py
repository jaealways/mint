#===================== 새로 짠 코드 ======================# 
# tweepy 패키지 import

import tweepy

# API 인증요청

consumer_key = "==="

consumer_secret = "==="

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)



# access 토큰 요청

access_token = "==="

access_token_secret = "==="

auth.set_access_token(access_token, access_token_secret)



# twitter API 생성  

api = tweepy.API(auth)

import os
import sys



location = "%s,%s,%s" % ("35.95", "128.25", "1000km")  # 검색기준(대한민국 중심) 좌표, 반지름  

keyword = "dynamite AND bts"                            # OR 로 검색어 묶어줌, 검색어 5개(반드시 OR 대문자로)                               # api 생성

wfile = open(os.getcwd()+"/twitter.txt", mode='w')     # 쓰기 모드



# twitter 검색 cursor 선언

cursor = tweepy.Cursor(api.search, q=keyword,

                       since='2021-05-09',  # 2015-01-01 이후에 작성된 트윗들로 가져옴

                       count=100,           # 페이지당 반환할 트위터 수 최대 100

                       geocode=location,    # 검색 반경 조건

                       include_entities=True)
                       
number = []
for i, tweet in enumerate(cursor.items()):
    number.append(i)
    print("{}: {}".format(i, tweet.text))  
    
len(number)    
