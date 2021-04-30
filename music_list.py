import requests
import re
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import pymysql
import pandas as pd

class MusicList():
    def __init__(self):

        self.crawl_link()

        self.name_classifier()

        self.collect_db()


    def crawl_link(self):
        print("뮤직카우  크롤링 시작")
        self.

    def name_classifier

    def collect_db