import requests
import re
from bs4 import BeautifulSoup
from pymongo import MongoClient

# 뮤직카우 초기 크롤링 정보 모으는 클래스


class MusicList:
    def __init__(self, num):
        # 뮤직카우 크롤링 시작
        self.num = num
        self.read_db()

    def read_db(self):
        # DB에 곡이 이미 존재하면 크롤링 하기 전에 패스
        is_exist = col.find({'num': self.num}, {'_id': 0, 'num': 1})
        if col.count_documents({'num': self.num}) != 0:
            # if 문에 for x in is_exist 쓰면 x가 존재하지 않는 경우(즉 db)에 저장된 값이 없으면 에러 발생
            for x in is_exist:
                if x['num'] == self.num:
                    print("{0}번 곡은 이미 DB에 존재합니다.".format(x['num']))
                    pass
                else:
                    raise IndexError
                    print("DB 입력 값 {0}과 홈페이지 넘버 {1}이 일치하지 않습니다.".format(x['num'], self.num))
        else:
            print("{0}번 곡은 DB에 없습니다. 크롤링 시작.".format(self.num))
            self.identify_link()

    def identify_link(self):
        self.page = "https://www.musicow.com/song/{0}?tab=info".format(self.num)
        self.url = requests.get(self.page)
        self.html = self.url.text
        self.soup = BeautifulSoup(self.html, 'html.parser')

        self.song_title = str(self.soup.select('div.song_header > div.information > p > strong'))
        self.song_title = re.sub('<.+?>', '', self.song_title, 0).replace('[', '').replace(']','').strip()

        print("{0}번 곡 확인 중".format(self.num))

        # 노래 제목이 없을 경우 비어있는 페이지이므로 pass
        if self.song_title[1:-1] == '':
            print("{0}번 곡은 존재하지 않습니다.".format(self.num))
            pass
        else:
            self.crawl_link()

    def crawl_link(self):
        print("{0}번 곡 뮤직카우 크롤링 시작".format(self.num))
        self.song_artist = str(self.soup.select('div.song_header > div.information > em'))
        self.song_artist = re.sub('\<.+?>|\[|\'|\]', '', self.song_artist, 0).strip()

        self.auc_date_1 = str(self.soup.select('div:nth-child(1) > h2 > small'))
        self.auc_date_1 = re.sub('\<.+?>|\[|\'|\]', '', self.auc_date_1, 0).strip()
        self.auc_date_1_start = self.auc_date_1.split('~')[0].strip()
        self.auc_date_1_end = self.auc_date_1.split('~')[1].strip()

        self.auc_stock_1 = str(self.soup.select('div.card_body > div > div:nth-child(1) > dl > dd:nth-child(2)'))
        self.auc_stock_1 = re.sub('\<.+?>|\[|\'|\]', '', self.auc_stock_1, 0).replace('주','').strip()
        self.auc_price_1 = str(self.soup.select('div.card_body > div > div:nth-child(1) > dl > dd:nth-child(8)'))
        self.auc_price_1 = re.sub('\<.+?>|\[|\'|\]|\,', '', self.auc_price_1, 0).replace('캐쉬','').strip()

        self.auc_date_2 = str(self.soup.select('div:nth-child(2) > h2 > small'))
        self.auc_date_2 = re.sub('\<.+?>|\[|\'|\]', '', self.auc_date_2, 0).strip()
        if self.auc_date_2[1:-1] == '':
            # None 말고 NaN과 0 이라는 값 넣은 이유는 string과 int로 type 통일하기 위해서
            self.auc_date_2_start = 'NaN'; self.auc_date_2_end = 'NaN'
            self.auc_stock_2 = 0; self.auc_price_2 = 0
        else:
            self.auc_date_2_start = self.auc_date_2.split('~')[0].strip()
            self.auc_date_2_end = self.auc_date_2.split('~')[1].strip()

        self.auc_stock_2 = str(self.soup.select('div.card_body > div > div:nth-child(2) > dl > dd:nth-child(2)'))
        self.auc_stock_2 = re.sub('\<.+?>|\[|\'|\]', '', self.auc_stock_2, 0).replace('주','').strip()
        self.auc_price_2 = str(self.soup.select('div.card_body > div > div:nth-child(2) > dl > dd:nth-child(8)'))
        self.auc_price_2 = re.sub('\<.+?>|\[|\'|\]|\,', '', self.auc_price_2, 0).replace('캐쉬','').strip()

        self.song_release_date = str(self.soup.select('div.card_body > div > dl > dd:nth-child(2)'))
        self.song_release_date = re.sub('\<.+?>|\[|\'|\]', '', self.song_release_date, 0).strip()

        # stock_num 앞뒤 공백 제거 후 숫자만 추출
        self.stock_num = str(self.soup.select('div.card_body > div > dl > dd:nth-child(20) > p:nth-child(1)'))
        self.stock_num = re.sub('\<.+?>|\[|\'|\]|\t|\n|\,', '', self.stock_num, 0).replace('1/','').strip()

        # self.classify_name()
        self.collect_db()

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

        col.insert_one(list_music).inserted_id


if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db = client.music_cow
    col = db.music_list

    for num in range(0, 2000):
        MusicList(num)
