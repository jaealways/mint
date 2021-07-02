from selenium import webdriver
import time
import pandas as pd
import numpy as np
import re
import requests
from bs4 import BeautifulSoup


headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"}
title='롤린'
artist='브레이브 걸스'
url='https://www.genie.co.kr/search/searchSong?query={0}&Coll='.format(title+' '+artist)
res=requests.get(url,headers=headers)
soup=BeautifulSoup(res.text, 'html.parser')
name=soup.select('tr a.title')
num=soup.select('a.btn-info')
count=0
    

for n, i in enumerate(name):
    if title.lower().replace(' ','') in i.text.lower().replace(' ',''):
        count+=1
        
        url=num[n].attrs['onclick']
        link='https://www.genie.co.kr/detail/songInfo?xgnm='+re.findall('\d+',url)[0]
        print(link)

