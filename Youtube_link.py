from bs4 import BeautifulSoup
from selenium import webdriver
import time
import google_crawler as fc

song_title = fc.song_title
song_artist = fc.song_artist

page_Youtube = "https://www.youtube.com/search?q={0}&sp=EgIQAQ%253D%253D".format(song_artist + ' ' + song_title)

driver = webdriver.Chrome()
driver.get(page_Youtube)

html_Youtube = driver.page_source
soup_Youtube = BeautifulSoup(html_Youtube, 'html.parser')
time.sleep(5)

# 상위 10개 관련성 동영상 매일 바뀔 수 있으므로 고윳값으로 저장

search_num_Youtube = soup_Youtube.select('a#video-title')


count = 0

for i in search_num_Youtube:
    count+=1
    href = i.attrs['href']
    href = "https://youtube.com{0}".format(href)
    print(count, href)

    if count == 10:
        break


driver.close()
