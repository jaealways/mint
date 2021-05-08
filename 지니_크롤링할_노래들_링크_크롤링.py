from selenium import webdriver
import time
import pandas as pd
import numpy as np
import re
import requests
from bs4 import BeautifulSoup

#데이터 로드
data=pd.read_excel('./지니링크크롤링용_제목_가수.xlsx')


title=data['노래']

#검색어가 담길 리스트 생성
searches=[]
for i in range(0,731):
    searches.append(data['노래'][i]+' '+data['아티스트'][i])
    
driver=webdriver.Chrome('./chromedriver')
driver.get('https://www.genie.co.kr/')


#셀레늄을 이용한 크롤링 코드
for no,search in enumerate(searches):


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
    name=soup.select('tr a.title') #검색 결과시 나오는 노래 제목들
    num=soup.select('a.btn-info') #상세정보로 이동하는 버튼
    
    
    # 검색결과 나오는 곡들의 제목에 excel 상의 제목이 포함되어 있다면 해당 링크만 출력
    for n,i in enumerate(name):
        if title[no].lower().replace(' ','') in i.text.lower().replace(' ',''):
            count+=1
        
            url=num[n].attrs['onclick'] #상세정보로 이동하는 버튼이 가진 고유 수 따오기 (링크의 규칙에 해당하는 숫자)
            link='https://www.genie.co.kr/detail/songInfo?xgnm='+re.findall('\d+',url)[0] #주어진 링크 + 고유의 수 가 그 곡의 상세페이지로 넘어가는 링크
            print(link)
    
  
            
    
    driver.back()
    time.sleep(0.1)
    driver.back()
    

