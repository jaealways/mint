from selenium import webdriver
import time
import pandas as pd
import numpy as np
import re
import requests
from bs4 import BeautifulSoup

driver=webdriver.Chrome('./chromedriver')
driver.get('https://www.genie.co.kr/')
df2=pd.read_excel('./가수이름분리.xlsx')
df3=pd.concat([df,df2],axis=1)
df3
del df3['Unnamed: 0']
for n,i in enumerate(df3['메인제목_eng']):
    if type(i)==float:
        df3['메인제목_eng'][n]='[]'
index1=df3[df3['메인제목_eng']!='[]']['메인제목_eng'].index
df3['노래제목키워드']=df3['메인제목_kor']
df3['노래제목키워드'][index1]=df3['메인제목_eng'][index1]
df3['가수이름키워드']=df3['메인 가수_kor']
index2=df3[df3['메인 가수_eng']!='[]'].index
df3['가수이름키워드'][index2]=df3['메인 가수_eng'][index2]
search_keyword=[]
title=df3['노래제목키워드']
for n,i in enumerate(range(0,len(df3))):
    search_keyword.append(df3['노래제목키워드'][n]+' '+ df3['가수이름키워드'][n])

for no,search in enumerate(search_keyword):


    box=driver.find_element_by_css_selector('input#sc-fd')
    box.click()
    time.sleep(0.5)

    box.send_keys(search)
    
    box=driver.find_element_by_css_selector('input.btn-submit')
    
    box.click()
    time.sleep(0.5)
    
    box=driver.find_element_by_css_selector('#body-content > div.tab-1.search_main > ul > li:nth-child(3) > a')
    box.click()
    time.sleep(0.5)


    

    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    name=soup.select('tr a.title')
    num=soup.select('a.btn-info')
    count=0
    
    #
    for n,i in enumerate(name):
        if title[no].lower().replace(' ','') in i.text.lower().replace(' ',''):
            count+=1
        
            url=num[n].attrs['onclick']
            link='https://www.genie.co.kr/detail/songInfo?xgnm='+re.findall('\d+',url)[0]
            #print(link)
    #print(count)
    if count == 0:
        print(searches[no],'\n',no)
            
    
    driver.back()
    time.sleep(0.1)
    driver.back()
