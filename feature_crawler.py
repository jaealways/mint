# 구글 검색 기사 개수
# 유튜브 조회수 관련성 영상 상위 10개 - 재형

import requests
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from urllib.request import urlopen


# from song_auc_crawler import song_title, song_artist
song_title = "친구라도 될 걸 그랬어"
song_artist = "거미"

#Google Crawling

page_Google = "https://www.google.com/search?q={0}".format(song_artist + ' ' + song_title)

driver = webdriver.Chrome()
driver.get(page_Google)

html_Google = driver.page_source
soup_Google = BeautifulSoup(html_Google, 'html.parser')

search_num_Google = soup_Google.select('div.LHJvCe')

for n in search_num_Google:
    search_Google = n.text.strip()
    print(search_Google)

driver.close()


# Youtube Crawling

# 관련성 기준으로 검색

page_Youtube = "https://www.youtube.com/search?q={0}&sp=EgIQAQ%253D%253D".format(song_artist + ' ' + song_title)

driver = webdriver.Chrome()
driver.get(page_Youtube)

html_Youtube = driver.page_source
soup_Youtube = BeautifulSoup(html_Youtube, 'html.parser')

search_num_Youtube = soup_Youtube.select('div.title-wrapper')

for n in search_num_Youtube:
    search_Youtube = n.text.strip()
    print(search_Youtube)

#driver.close()







