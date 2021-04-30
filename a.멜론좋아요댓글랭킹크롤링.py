# from selenium import webdriver
# import time
# import pandas as pd
# import numpy as np

# driver=webdriver.Chrome('./chromedriver')
# driver.get('https://www.melon.com/')


# for num, title in enumerate(title):
    
#     box=driver.find_element_by_css_selector('#top_search')
#     box.click()
#     time.sleep(1)

#     box.send_keys(title)
    
#     box=driver.find_element_by_css_selector('#gnb > fieldset > button.btn_icon.search_m > span')
#     box.click()
#     time.sleep(2)
#     box=driver.find_elements_by_css_selector('form#frm_searchSong tbody tr a span.odd_span')
#     length=len(box)
#     for i in range(0,length):
#         box=driver.find_elements_by_css_selector('form#frm_searchSong tbody tr a span.odd_span')[i]
#         box.click()
#         time.sleep(1)
#         likes=driver.find_element_by_css_selector('span#d_like_count').text
#         reply=driver.find_element_by_css_selector('span#revCnt').text
#         try: 
#             rank=driver.find_element_by_css_selector('div.chart span.num').text
#         except: 
#             rank=np.NaN
#         print(num,' ', title,likes,' ',reply,' ',rank)
#         driver.back()
#         time.sleep(1)
    
#     driver.back()

import re
from bs4 import BeautifulSoup
from selenium import webdriver
import time

driver=webdriver.Chrome('./chromedriver')
driver.get('https://www.melon.com/')

    
box=driver.find_element_by_css_selector('#top_search')
box.click()
time.sleep(1)
title='친구라도 될 걸 그랬어 거미'
tem='친구라도 될 걸 그랬어'
box.send_keys(title)
box=driver.find_element_by_css_selector('#gnb > fieldset > button.btn_icon.search_m > span')
box.click()
time.sleep(1)
html = driver.page_source
soup = BeautifulSoup(html, 'html.parser')
box=soup.select('tbody tr')
url='https://www.melon.com/song/detail.htm?songId='
for i in box:
    try:
        name=i.select_one('a.fc_gray').text
        temp=i.select_one('a.btn_icon_detail')
        num=re.findall('\d+',temp.attrs['href'])[0]
    except:
        pass
    
    if tem.replace(' ','') in name.replace(' ',''):
        print(url+num)
    


    

