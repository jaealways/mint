import requests
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from pymongo import MongoClient
import time


class WebCrawler():
    def __init__(self):
        self.read_db()
        self.crawling_youtube()



    def read_db(self):
        client = MongoClient('localhost', 27017)
        db = client.music_cow
        db.myCol
        posts = db.posts


    def crawling_youtube(self):





    def crawling_news(self):
        comb = str(self.artist + ' ' + self.title)

        for num in comb:
             page = self.link.format(num)
             url = requests.get(page)
             html = url.text
             soup = BeautifulSoup(html, "html.parser")

             self.list = str(soup.select(self.tag))
             self.list = re.sub('<.+?>', '', 0).strip()



if __name__ == '__main__':
    driver = webdriver.Chrome()
    WebCrawler()