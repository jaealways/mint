# 유튜브 관련 동영상 순위는 매일 바뀔 수 있으므로 초기에 고정된 값 리스트로 따로 뽑는다.

import openpyxl
from bs4 import BeautifulSoup
import requests
import re
from selenium import webdriver
import time
import temp_crawler as tc


matching = tc.matching



for songs in matching:
    page_Youtube = "https://www.youtube.com/search?q={0}&sp=EgIQAQ%253D%253D".format(songs)
    driver = webdriver.Chrome()
    driver.get(page_Youtube)

    html_Youtube = driver.page_source
    soup_Youtube = BeautifulSoup(html_Youtube, 'html.parser')
    time.sleep(10)

    search_num_Youtube = soup_Youtube.select('a#video-title')

    count = 0
    list = []

    print(songs)

    for i in search_num_Youtube:
        count+=1
        href = i.attrs['href']
        href = "https://youtube.com{0}".format(href)
        print(count, href)

        if count == 10:
            break

    time.sleep(10)
    driver.close()


#
# wb = openpyxl.Workbook()
# sheet = wb.active
# sheet.append(['제목','가수','video1','video2','video3','video4','video5','video6','video7','video8','video9','video10'])
#
# count = 0
#
# for num in list:
#     count+=1
#     href = num.attrs['href']
#     href = "https://youtube.com{0}".format(href)
#
#
#
#
#     sheet.append([song_title[1:-1], song_artist[1:-1], auc_date_1[1:-1], auc_stock_1[1:-1], auc_price_1[1:-1], auc_date_2[1:-1], auc_stock_2[1:-1], auc_price_2[1:-1], song_date[1:-1], ''.join(re.findall("\d", stock_num))[1:], page])
#
# wb.save('market2.xlsx')