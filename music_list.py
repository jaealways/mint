import requests
import re
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import pymysql
import pandas as pd

class MusicList():
    def __init__(self):
        # 뮤직카우 크롤링 시작
        self.crawl_link()
        # 아티스트랑 타이틀 분류하기(feat, prod)
        self.name_classifier()
        # DB에 저장하기
        self.collect_db()


    def crawl_link(self):
        print("뮤직카우 크롤링 시작")
        for num in range(0,2000):
            self.page = "https://www.musicow.com/song/{0}?tab=info".format(num)
            self.html = BeautifulSoup((requests.get(self.page)).text, 'html.parser')
            self.


    def name_classifier

    def collect_db