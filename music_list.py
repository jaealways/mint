import requests
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from pymongo import MongoClient
import time


class MusicList():
    def __init__(self):
        # 뮤직카우 크롤링 시작
        self.crawl_link()


    def crawl_link(self):
        print("뮤직카우 크롤링 시작")
        for self.num in range(0,2000):
            self.page = "https://www.musicow.com/song/{0}?tab=info".format(self.num)
            self.url = requests.get(self.page)
            self.html = self.url.text
            self.soup = BeautifulSoup(self.html, 'html.parser')

            self.song_title = str(self.soup.select('div.song_header > div.information > p > strong'))
            self.song_title = re.sub('<.+?>', '', self.song_title, 0).strip().replace('[', '').replace(']','')

            print("{0}번째 노래 저장 중".format(self.num))

            #노래 제목이 없을 경우 비어있는 페이지이므로 pass
            if self.song_title[1:-1]=='':
                pass
            else:

                #위에서 pass되지 않은 곡만 기록
                #re.sub -> 정규표현식
                self.song_artist = str(self.soup.select('div.song_header > div.information > em'))
                self.song_artist = re.sub('<.+?>', '', self.song_artist, 0).strip().replace('[', '').replace(']','')

                self.auc_date_1 = str(self.soup.select('div:nth-child(1) > h2 > small'))
                self.auc_date_1 = re.sub('<.+?>', '', self.auc_date_1, 0).strip().replace('[', '').replace(']','')
                self.auc_stock_1 = str(self.soup.select('div.card_body > div > div:nth-child(1) > dl > dd:nth-child(2)'))
                self.auc_stock_1 = re.sub('<.+?>', '', self.auc_stock_1, 0).strip().replace('[', '').replace(']','')
                self.auc_price_1 = str(self.soup.select('div.card_body > div > div:nth-child(1) > dl > dd:nth-child(8)'))
                self.auc_price_1 = re.sub('<.+?>', '', self.auc_price_1, 0).strip().replace('[', '').replace(']','')

                self.auc_date_2 = str(self.soup.select('div:nth-child(2) > h2 > small'))
                self.auc_date_2 = re.sub('<.+?>', '', self.auc_date_2, 0).strip().replace('[', '').replace(']','')
                self.auc_stock_2 = str(self.soup.select('div.card_body > div > div:nth-child(2) > dl > dd:nth-child(2)'))
                self.auc_stock_2 = re.sub('<.+?>', '', self.auc_stock_2, 0).strip().replace('[', '').replace(']','')
                self.auc_price_2 = str(self.soup.select('div.card_body > div > div:nth-child(2) > dl > dd:nth-child(8)'))
                self.auc_price_2 = re.sub('<.+?>', '', self.auc_price_2, 0).strip().replace('[', '').replace(']','')

                self.song_date = str(self.soup.select('div.card_body > div > dl > dd:nth-child(2)'))
                self.song_date = re.sub('<.+?>', '', self.song_date, 0).strip().replace('[', '').replace(']','')

                #stock_num 앞뒤 공백 제거 후 숫자만 추출(어떻게 간추릴 수 있을지 고민 중)
                self.stock_num = str(self.soup.select('div.card_body > div > dl > dd:nth-child(20) > p:nth-child(1)'))
                self.stock_num = re.sub('<.+?>', '', self.stock_num, 0).strip()
                self.stock_num = self.stock_num.replace('\t','').replace('\n','').replace('1/','').replace(',','').replace('[', '').replace(']','')

                # self.translating_naver()
                # self.classify_name()
                self.listing_youtube()
                self.collect_db()


    def translating_naver(self):
        self.pair = self.song_artist + ' ' + self.song_title
        self.pair = self.pair.replace('[', '').replace(']','')
        self.naver = "https://vibe.naver.com/search?query={0}".format(self.pair)
        self.url_naver = requests.get(self.naver)
        self.html_naver = self.url_naver.text
        self.soup_naver = BeautifulSoup(self.html_naver, 'html.parser')

        print("NAVER VIBE로 변환 중")
        self.song_title = str(self.soup.select('div:nth-child(1) > div.info_area > div.title > span.inner > a'))
        self.song_title = re.sub('<.+?>', '', self.song_title, 0).strip().replace('[', '').replace(']','')
        self.song_artist = str(self.soup.select('div:nth-child(1) > div.info_area > div.artist > span:nth-child(1) > span > a > span'))
        self.song_artist = re.sub('<.+?>', '', self.song_artist, 0).strip().replace('[', '').replace(']','')


    def classify_name(self):
        # Feat 분류
        if 'Feat.' in self.song_title:
            pos_1 = re.search('Feat.', self.song_title)



        # Prod 분류

        # 메인 가수, 서브 가수 한글, 영어


        # 원제 한글 영어
        self.song


    def listing_youtube(self):
        page_Youtube = "https://www.youtube.com/search?q={0}&sp=EgIQAQ%253D%253D".format(self.pair)

        driver.get(page_Youtube)

        html_Youtube = driver.page_source
        soup_Youtube = BeautifulSoup(html_Youtube, 'html.parser')
        time.sleep(5)

        search_num_Youtube = soup_Youtube.select('a#video-title')
        count = 0
        self.youtube_video = []

        for i in search_num_Youtube:
            count+=1
            self.href = i.attrs['href']
            self.href = "https://youtube.com{0}".format(self.href)
            print(count, self.href)
            self.youtube_video.append(self.href)
            time.sleep(0.7)

            if count == 10:
                break


    def collect_db(self):
        client = MongoClient('localhost', 27017)
        db = client.music_cow
        db.myCol
        posts = db.posts

        print("{0}번째 노래 DB 입력 중".format(self.num))


        music={
            'song_title': self.song_title,
            'num': self.num,
            'page': self.page,
            'song_artist': self.song_artist,
            'auc1_info':{
                'auc_date': self.auc_date_1, 'auc_stock': self.auc_stock_1, 'auc_price': self.auc_price_1},
            'auc2_info':{
                'auc_date': self.auc_date_2, 'auc_stock': self.auc_stock_2, 'auc_price': self.auc_price_2},
            'auc_song_date': self.song_date,
            'stock_num': self.stock_num,
            'youtube_video': self.youtube_video
        }


        posts.insert_one(music).inserted_id



if __name__ == '__main__':
    driver = webdriver.Chrome()
    MusicList()

driver.close()