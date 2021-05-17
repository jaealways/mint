import requests
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from pymongo import MongoClient
import time


class YoutubeList:
    def __init__(self):
        self.num = num
        self.read_db()

    def read_db(self):
        # DB에 곡이 있으면 유튜브 리스트 크롤링 시작
        music_exist = music_list.find({'num': self.num})
        if music_exist[1:-1] == '':
            pass
        else:


            self.listing_youtube()

    def listing_youtube(self):
        self.pair = self.song_artist + ' ' + self.song_title
        page_youtube = "https://www.youtube.com/search?q={0}&sp=EgIQAQ%253D%253D".format(self.pair)

        driver.get(page_youtube)

        html_youtube = driver.page_source
        soup_youtube = BeautifulSoup(html_youtube, 'html.parser')
        time.sleep(5)

        search_num_Youtube = soup_youtube.select('a#video-title')
        count = 0
        self.youtube_video = []
        print("Youtube List 출력 중")

        for i in search_num_Youtube:
            count+=1
            self.href = i.attrs['href']
            self.href = "https://youtube.com{0}".format(self.href)
            print(count, self.href)
            self.youtube_video.append(self.href)
            time.sleep(0.7)

            if count == 10:
                break


if __name__ == '__main__':
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_arguemnt('--incognito')
    driver = webdriver.Chrome(options=chrome_options)

    client = MongoClient('localhost', 27017)
    db = client.music_cow
    music_list = db.music_list
    youtube_list = db.youtube_list

    for num in range(0,2000):
        YoutubeList(num)

    driver.close()

