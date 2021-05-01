import requests
import re
from bs4 import BeautifulSoup


class HtmlCrawler():
    def __init__(self):
        self.input_info()
        self.variable_setting()


    def input_info(self):
        self.artist = str(input("아티스트 입력: "))
        self.title = str(input("제목 입력: "))
        self.link = (str(input("링크 입력(link는 {0}.format(num)형태로 넣을것): "))).format(num)
        self.tag = str(input("태그 입력: "))



    # link는 {0}.format(num)형태로 넣을것
    # comb는 노래 제목과 아티스트 조합한 str
    # tag는 크롤링 원하는 tag 입력하여 name으로 저장

    def variable_setting(self):
        comb = str(self.artist + ' ' + self.title)

        for num in comb:
             page = self.link.format(num)
             url = requests.get(page)
             html = url.text
             soup = BeautifulSoup(html, "html.parser")

             self.list = str(soup.select(self.tag))
             self.list = re.sub('<.+?>', '', 0).strip()



result = HtmlCrawler()
result.list
