# 구글 검색 기사 개수
# 유튜브 조회수 관련성 영상 상위 10개 - 재형

import requests
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from urllib.request import urlopen
import lxml
import time
import Youtube_link as yl


# # from song_auc_crawler import song_title, song_artist
song_title = "dynamite"
song_artist = "bts"
# #
# # Google Crawling
#
# page_Google = "https://www.google.com/search?q={0}&source=lnms&tbm=nws&tbs=qdr:d&sa=X&ved=2ahUKEwihz8TrjfXvAhUjNKYKHZLKAjkQ_AUoA3oECAEQBQ&biw=767&bih=700".format(song_artist + ' ' + song_title)
#
# driver = webdriver.Chrome()
# driver.get(page_Google)
#
# html_Google = driver.page_source
# soup_Google = BeautifulSoup(html_Google, 'html.parser')
#
# search_num_Google = soup_Google.select('div.LHJvCe')
#
# for n in search_num_Google:
#     search_Google = n.text.strip()
#     print(search_Google)
#
# driver.close()


# Youtube Crawling

link = yl.href
print(link)
