# 구글 검색 기사 개수
# 유튜브 조회수 관련성 영상 상위 10개 - 재형

import requests
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from urllib.request import urlopen
import lxml


# from song_auc_crawler import song_title, song_artist
song_title = "친구라도 될 걸 그랬어"
song_artist = "거미"
#
# #Google Crawling
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

# driver.close()


# page_Youtube = "https://www.youtube.com/search?q={0}&sp=EgIQAQ%253D%253D".format(song_artist + ' ' + song_title)
#
#
# search_Youtube_info = {
#     'title':'',
#     'video_link':'',
#     'hits' : '',
#     'updated_time':''
# }
#
# def search_Youtube(page_Youtube):
#     response = requests.get(page_Youtube)
#     soup = BeautifulSoup(response.text, "lxml")
#     lis = soup.find_all('meta', {'class' : 'style-scope ytd-video-render'})
#     for li in lis :
#         title = li.find('a', {'title' : True})['title']
#         video_link = 'https://www.youtube.com' + li.find('a', {'href' : True})['href']
#         hits = li.find_all('li')[2].text
#         updated_time = li.find_all('li')[3].text
#         search_Youtube_info = {
#             'title' : title,
#             'video_link' : video_link,
#             'hits' : hits,
#             'updated_time' : updated_time
#         }
#         print(search_Youtube_info)
#     return search_Youtube_info
#
# search_Youtube(page_Youtube)





