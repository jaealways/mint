from pymongo import MongoClient
from datetime import datetime
import re
import urllib.request as request
from bs4 import BeautifulSoup
from bson.json_util import dumps, loads
import subprocess as cmd
import json

class MusicCowDailyCrawler:
    def __init__(self):
        self.add_db()  #######
        # self.read_db()

#########################################
    def add_db(self):
        list_db_music = col1.find({}, {'num': {"$slice": [1, 1]}})

        for i in range(0,2500):

            html = request.urlopen("https://www.musicow.com/song/{0}?tab=price".format(i))
            soup = BeautifulSoup(html, "html.parser")

            title = soup.find('p', attrs={'class': 'title'}).find('strong')

            if title != '':
                for j in list_db_music:
                    if i == j['num']:
                        pass
                    else:
                        print("{0} 번곡은 music_list db에 없음. 신곡입니다.".format(i))
                        print("{0} 곡을 music_list 에 추가합니다.")
                        self.crawl_link()

    def crawl_link(self):
        print("{0}번 곡 뮤직카우 크롤링 시작".format(self.num))
        self.song_artist = str(self.soup.select('div.song_header > div.information > em'))
        self.song_artist = re.sub('\<.+?>|\[|\'|\]', '', self.song_artist, 0).replace('&amp;', '&').strip()

        self.auc_date_1 = str(self.soup.select('div:nth-child(1) > h2 > small'))
        self.auc_date_1 = re.sub('\<.+?>|\[|\'|\]', '', self.auc_date_1, 0).strip()
        self.auc_date_1_start = self.auc_date_1.split('~')[0].strip()
        self.auc_date_1_end = self.auc_date_1.split('~')[1].strip()

        self.auc_stock_1 = str(self.soup.select('div.card_body > div > div:nth-child(1) > dl > dd:nth-child(2)'))
        self.auc_stock_1 = re.sub('\<.+?>|\[|\'|\]|\,', '', self.auc_stock_1, 0).replace('주','').strip()
        self.auc_price_1 = str(self.soup.select('div.card_body > div > div:nth-child(1) > dl > dd:nth-child(8)'))
        self.auc_price_1 = re.sub('\<.+?>|\[|\'|\]|\,', '', self.auc_price_1, 0).replace('캐쉬','').strip()

        self.auc_date_2 = str(self.soup.select('div:nth-child(2) > h2 > small'))
        self.auc_date_2 = re.sub('\<.+?>|\[|\'|\]', '', self.auc_date_2, 0).strip()

        if self.auc_date_2[0:-1] == '':
            # None 말고 NaN과 0 이라는 값 넣은 이유는 string과 int로 type 통일하기 위해서
            self.auc_date_2_start = 'NaN'; self.auc_date_2_end = 'NaN'
            self.auc_stock_2 = 0; self.auc_price_2 = 0
        else:
            self.auc_date_2_start = self.auc_date_2.split('~')[0].strip()
            self.auc_date_2_end = self.auc_date_2.split('~')[1].strip()

        self.auc_stock_2 = str(self.soup.select('div.card_body > div > div:nth-child(2) > dl > dd:nth-child(2)'))
        self.auc_stock_2 = re.sub('\<.+?>|\[|\'|\]|\,', '', self.auc_stock_2, 0).replace('주','').strip()
        self.auc_price_2 = str(self.soup.select('div.card_body > div > div:nth-child(2) > dl > dd:nth-child(8)'))
        self.auc_price_2 = re.sub('\<.+?>|\[|\'|\]|\,', '', self.auc_price_2, 0).replace('캐쉬','').strip()

        self.song_release_date = str(self.soup.select('div.card_body > div > dl > dd:nth-child(2)'))
        self.song_release_date = re.sub('\<.+?>|\[|\'|\]', '', self.song_release_date, 0).strip()

        # stock_num 앞뒤 공백 제거 후 숫자만 추출
        self.stock_num = str(self.soup.select('div.lst_copy_info dd p')).split('2차적')[0]
        self.stock_num = re.sub('\<.+?>|\[|\'|\]|\t|\n|\,', '', self.stock_num, 0).replace('1/','').strip()

        #self.classify_name()
        self.collect_db()

    #def classify_name(self):


    def collect_db(self):
        print("{0}번 곡 DB 입력 중".format(self.num))

        list_music = {
            'num': self.num,
            'song_title': self.song_title,
            'song_artist': self.song_artist,
            'page': self.page,
            'auc1_info': {
                'auc_start_date': self.auc_date_1_start, 'auc_end_date': self.auc_date_1_end,
                'auc_stock': self.auc_stock_1, 'auc_price': self.auc_price_1},
            'auc2_info': {
                'auc_start_date': self.auc_date_2_start, 'auc_end_date': self.auc_date_2_end,
                'auc_stock': self.auc_stock_2, 'auc_price': self.auc_price_2},
            'song_release_date': self.song_release_date,
            'stock_num': self.stock_num
        }


    def read_db(self):
        list_db_music = col1.find({}, {'num': {"$slice": [1, 1]}})
        for x in list_db_music:
            self.num = x['num']
            if self.num < 0:
                pass
            else:
                if 'song_artist_main_kor1' in x['list_split']:
                    self.song_artist = x['list_split']['song_artist_main_kor1']
                else:
                    self.song_artist = x['list_split']['song_artist_main_eng1']
                if 'song_title_main_kor' in x['list_split']:
                    self.song_title = x['list_split']['song_title_main_kor']
                else:
                    self.song_title = x['list_split']['song_title_main_eng']
                self.pair = self.song_artist + ' ' + self.song_title

                self.crawling_daily()

    def crawling_daily(self):
        html = request.urlopen("https://www.musicow.com/song/{0}?tab=price".format(self.num))
        soup = BeautifulSoup(html, "html.parser")

        price = soup.find('strong', attrs = {'class' : 'amt_market_latest'})
        title = soup.find('p', attrs = {'class' : 'title'}).find('strong')

        self.price = price.get_text().replace(',','').replace('캐쉬','').strip()

        print("{}번곡 {}".format(self.num, title.get_text()))
        print("                                현재가 : {}".format(self.price))

        self.list_genie = {
            'num': self.num,
            'song_title': self.song_title,
            'song_artist': self.song_artist,
        }

        date_today = datetime.now().strftime('%Y-%m-%d')

        daily_music_cow_list = {
            'price': self.price
        }

        self.list_genie['{0}'.format(date_today)] = daily_music_cow_list
        print(daily_music_cow_list)

        col2.insert_one(self.list_genie).inserted_id



if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    db2 = client.daily_crawler
    col1 = db1.music_list_split
    col2 = db2.daily_music_cow

    MusicCowDailyCrawler()


# class MusicList_add:
#     def __init_(self, num):
#         # 뮤직카우 크롤링 시작
#         self.num = num
#         self.read_db()
#
#     def crawling_add(self):
#         for i in range(0, 2500):
#             html = urlopen("https://www.musicow.com/song/{0}?tab=price".format(i))
#             soup = BeautifulSoup(html, "html.parser")
#
#             price = soup.find('strong', attrs={'class': 'amt_market_latest'})
#             title = soup.find('p', attrs={'class': 'title'}).find('strong')
#
#             self.price = price.get_text().replace(',', '').replace('캐쉬', '').strip()
#
#             print("{}번곡 {}".format(self.num, title.get_text()))
#             print("                                현재가 : {}".format(self.price))
#
#             self.list_add = {
#                 'num': self.num,
#                 'song_title': self.song_title,
#                 'song_artist': self.song_artist,
#             }
#
#     def read_db(self):
#         #DB에 곡이 이미 존재하면 크롤링 하기 전에 패스
#         list_db_music = col1.find({}, {'num': {"$slice": [1, 1]}})
#         for x in list_db_musin:
#             if
#
#
# if __name__ == '__main__':
#     client = MongoClient('localhost',27017)
#     db1 = client.music_cow
#     db2 = client.daily_crawler
#     col1 = db1.music_list
#     col2 = db2.music_list
#
#     for num in range(0, 2500):
#         MusicList_add(num)
#
#
# for i in range(0,2500):
#     html = urlopen("https://www.musicow.com/song/{0}?tab=price".format(i))
#     soup = BeautifulSoup(html, "html.parser")
#
#     price = soup.find('strong', attrs = {'class' : 'amt_market_latest'})
#     title = soup.find('p', attrs = {'class' : 'title'}).find('strong')
#
#     self.price = price.get_text().replace(',','').replace('캐쉬','').strip()
#
#     print("{}번곡 {}".format(self.num, title.get_text()))
#     print("                                현재가 : {}".format(self.price))