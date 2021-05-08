import tweepy
from tweepy import OAuthHandler, API
import pandas as pd

# 트위터 Application에서 발급 받은 key 정보들
consumer_key='P4Vhqlcf2qOHibWiC0EXYpCnL'
consumer_secret='UVTDOrO55Ay5yoVHUOSuqePLjndgsR32aErhgfsm3zGyLEJQKW'
access_token='1182506328696078336-rVMMKtibciWg8VQvFBT8YBJwZFEgYU'
access_token_secret='241AArB9BXCkeLFHQQpMk01FJgRRugOreQESWO7T6DmJx'

# 1. 핸들러 생성 및 개인정보 인증요청
auth=tweepy.OAuthHandler(consumer_key, consumer_secret)

# 2. 액세스 요청
auth.set_access_token(access_token, access_token_secret)

# 3. twitter API 생성
api = tweepy.API(auth,wait_on_rate_limit=True)

# 검색하고 싶은 키워드 입력
keyword='김재환 시간이필요해' 
result = []

for i in range(1,10):
    tweets=api.search(keyword)
    for tweet in tweets:
        result.append(tweet)  # 크롤링 결과가 리스트에 삽입
        
print(len(result)) # 크롤링 하여 가져온 트윗 개수
