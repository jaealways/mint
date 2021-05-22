import requests
import re
from bs4 import BeautifulSoup
title='롤린'
artist='브레이브 걸스'
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"}
url='https://search.naver.com/search.naver?where=news&query={0}&sm=tab_opt&sort=1&photo=0&field=0&pd=4&ds=&de=&docid=&related=0&mynews=0&office_type=0&office_section_code=0&news_office_checked=&nso=so%3Add%2Cp%3A1d'.format(title+artist)
url_poly='https://search.naver.com/search.naver'

res=requests.get(url, headers= headers)
soup=BeautifulSoup(res.text, 'html.parser')

pages=soup.select('div.sc_page_inner a')
count=0
try:
    if len(pages)<=1:
        
        
        
        cons=soup.select('ul.list_news li')
        for con in cons:
            
            try:
                temp=con.select('a.info')[1]
                link=temp.attrs['href']
                temp_res=requests.get(link, headers=headers)
                temp_soup=BeautifulSoup(temp_res.text, 'html.parser')
                text=temp_soup.select('div.end_body_wrp>div')
                #print(text)
            
                count+=1
                
                if temp_soup==[]:
                    print(link)
                
            except:
                temp=con.select_one('a.news_tit')
                link=temp.attrs['href']
                temp_res=requests.get(link, headers=headers)
                temp_soup=BeautifulSoup(temp_res.text, 'html.parser')
                #text=temp_soup.select('div#article-body')
                #print(temp_soup)
                count+=1
                
                if temp_soup==[]:
                    print(link)
        print(count)
            
    
    else:
        for page in pages:
            link=page.attrs['href']
            temp_res=requests.get(url_poly+link, headers=headers)
            temp_soup=BeautifulSoup(temp_res.text, 'html.parser')
            cons=temp_soup.select('ul.list_news li')
            for con in cons:
            
                try:
                    temp=con.select('a.info')[1]
                    link=temp.attrs['href']
                    temp_res=requests.get(link, headers=headers)
#                     temp_soup=BeautifulSoup(temp_res.text, 'html.parser')
#                     text=temp_soup.select('div.end_body_wrp>div')
                    #print(text)
            
                    count+=1
                    
                    if temp_soup==[]:
                        print(link)
                
                except:
                    temp=con.select_one('a.news_tit')
                    link=temp.attrs['href']
                    temp_res=requests.get(link, headers=headers)
                    temp_soup=BeautifulSoup(temp_res.text, 'html.parser')
                    #text=temp_soup.select('div#article-body')
                    #print(temp_soup)
                    count+=1
                    
                    if temp_soup==[]:
                        print(link)
        print(count)
            
except:
    pass
    

        


