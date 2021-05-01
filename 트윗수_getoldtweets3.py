#!/usr/bin/env python
# coding: utf-8

# In[1]:


#====================== 트위터 크롤링 (트윗 수) ========================#
# GetOldTweet3 사용 준비
try:
    import GetOldTweets3 as got
except:
    get_ipython().system('pip install GetOldTweets3')
    import GetOldTweets3 as got


# In[2]:


# BeautifulSoup4 사용 준비
try:
    from bs4 import BeautifulSoup
except:
    get_ipython().system('pip install bs4')
    from bs4 import BeautifulSoup


# In[9]:


# 가져올 범위를 정의
# 예제 : 2021-04-21 ~ 2019-04-24

import datetime

days_range = []

start = datetime.datetime.strptime("2019-12-21", "%Y-%m-%d")
end = datetime.datetime.strptime("2019-12-23", "%Y-%m-%d")

date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]

for date in date_generated:
    days_range.append(date.strftime("%Y-%m-%d"))

print("=== 설정된 트윗 수집 기간은 {} 에서 {} 까지 입니다 ===".format(days_range[0], days_range[-1]))
print("=== 총 {}일 간의 데이터 수집 ===".format(len(days_range)))


# In[18]:


# 특정 검색어가 포함된 트윗 검색하기 (quary search)
# 검색어 : 어벤져스, 스포

import time

# 수집 기간 맞추기
start_date = days_range[0]
end_date = (datetime.datetime.strptime(days_range[-1], "%Y-%m-%d") 
            + datetime.timedelta(days=1)).strftime("%Y-%m-%d") # setUntil이 끝을 포함하지 않으므로, day + 1

# 트윗 수집 기준 정의
tweetCriteria = got.manager.TweetCriteria().setQuerySearch('어벤져스 OR 스포')                                           .setSince(start_date)                                           .setUntil(end_date)                                           .setMaxTweets(-1)

# 수집 with GetOldTweet3
print("Collecting data start.. from {} to {}".format(days_range[0], days_range[-1]))
start_time = time.time()

tweet = got.manager.TweetManager.getTweets(tweetCriteria)

print("Collecting data end.. {0:0.2f} Minutes".format((time.time() - start_time)/60))
print("=== Total num of tweets is {} ===".format(len(tweet)))

