import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from pymongo import MongoClient
import time
import json


class YoutubeList:
    def __init__(self, num):
        self.num = num
        self.read_db()

    def read_db(self):
        list_db_music = col1.find({}, {'num': {"$slice": [self.num, 1]}})
        for x in list_db_music:
            self.song_artist = x['song_artist']
            self.song_title = x['song_title']
            self.pair = self.song_artist + ' ' + self.song_title

            self.listing_youtube()

    def listing_youtube(self):
        page_youtube = "https://www.youtube.com/search?q={0}&sp=EgIQAQ%253D%253D".format(self.pair)
        driver.get(page_youtube)

        html_youtube = driver.page_source
        soup_youtube = BeautifulSoup(html_youtube, 'html.parser')
        time.sleep(5)

        search_num_youtube = soup_youtube.select('a#video-title')
        count = 1
        print("{0} Youtube List 출력 중".format(self.pair))
        self.video_list = pd.DataFrame({'title': [], 'link': []})

        for i in search_num_youtube:
            self.href = i.attrs['href']
            self.href = "https://youtube.com{0}".format(self.href)
            self.title = i.attrs['aria-label']
            self.title = self.title.split('게시자')[0].strip()
            print("{0}의 {1}번째 비디오".format(self.title, count))
            time.sleep(0.7)

            insert_data = pd.DataFrame({'title': [self.title], 'link': [self.href]})
            self.video_list = self.video_list.append(insert_data)

            count += 1
            if count == 10:
                break

        print("{0} 비디오 DB 입력 중".format(self.pair))
        self.collect_db()

    # 데이터 프레임 db에 어떻게 입력하는가??
    # song 고유 식별자 부분은 처음에 입력하고, video list 는 for 문 안으로 집어넣음



    def collect_db(self):
        list_video_info = {
            'num': self.num,
            'song_title': self.song_title,
            'song_artist': self.song_artist,
            }

        # col2.insert_one(list_video_info).inserted_id

        self.video_list.reset_index(inplace=True)
        self.video_list_dict = self.video_list.to_dict()

        # list_video = {
        #     'video_info': {
        #         'video_title': self.video_list['title'], 'video_link': self.video_list['link']
        #     }
        # }

        col2.insert_many(list_video_info, self.video_list_dict)


if __name__ == '__main__':
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--incognito')
    driver = webdriver.Chrome(options=chrome_options)

    client = MongoClient('localhost', 27017)
    db = client.music_cow
    col1 = db.music_list
    col2 = db.youtube_list

    num_music = col1.count_documents({})

    for num in range(1, num_music + 1):
        YoutubeList(num)

    driver.close()

