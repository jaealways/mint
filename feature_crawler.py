# 구글 검색 기사 개수
# 유튜브 조회수 관련성 영상 상위 10개 - 재형

import requests
import re
from bs4 import BeautifulSoup
from selenium import webdriver

# from song_auc_crawler import song_title, song_artist
song_title = "친구라도 될 걸 그랬어"
song_artist = "거미"

#Google Crawling

page_Google = "https://www.google.com/search?q={0}".format(song_artist + ' ' + song_title)

driver = webdriver.Chrome()
driver.get(page_Google)

html_Google = driver.page_source
soup_Google = BeautifulSoup(html_Google, 'html.parser')


#Youtube Crawling

# page_Youtube = "https://www.youtube.com/search?q={0}&sp=EgIQAQ%253D%253D".format(song_artist + ' ' + song_title)
#
# url_Youtube = requests.get(page_Youtube)
# html_Youtube = url_Youtube.text
# soup_Youtube = BeautifulSoup(html_Youtube, 'html.parser')



#목표는 검색 갯수 나오게하는 것 41,300개 하지만 결과론 계속 '친구라도 될 걸 그랬어'가 나옴
#div.result-stats랑 기타 가능한 것 다 시도해봄
search_num_Google = str(soup_Google.select('div.LHJvCe > div.result-stats'))
search_num_Google = re.sub('<.+?>','',song_title,0).strip()

print(search_num_Google)



search_list_Youtube = str(soup_Youtube.select('div.title-wrapper > h3.title-and-badge style-scope ytd-video-renderer > a.video-title'))
search_list_Youtube = re.sub('<.+?>','',song_title,0).strip()

#print(search_list_Youtube)


