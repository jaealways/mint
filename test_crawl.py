from selenium import webdriver
from bs4 import BeautifulSoup

# setup Driver|Chrome : 크롬드라이버를 사용하는 driver 생성
driver = webdriver.Chrome()
driver.implicitly_wait(3) # 암묵적으로 웹 자원을 (최대) 3초 기다리기

driver.get('https://news.naver.com') # Naver 페이 들어가기
html = driver.page_source # 페이지의 elements모두 가져오기
soup = BeautifulSoup(html, 'html.parser') # BeautifulSoup사용하기
notices = soup.select('div.com_header')

for n in notices:
    print(n.text.strip())