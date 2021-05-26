from bs4 import BeautifulSoup
from selenium import webdriver
from pymongo import MongoClient
import time

class YoutubeList:
    def __init__(self):
        self.read_db()

    def read_db(self):
        list_db_music = col1.find({}, {'num': {"$slice": [1, 1]}})
        for x in list_db_music:
            self.num = x['num']
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

        self.list_video = {
            'num': self.num,
            'song_title': self.song_title,
            'song_artist': self.song_artist,
        }

        for i in search_num_youtube:
            self.href = i.attrs['href']
            self.href = "https://youtube.com{0}".format(self.href)
            self.title = i.attrs['aria-label']
            self.title = self.title.split('게시자')[0].strip()
            print("{0}의 {1}th - {2}".format(self.pair, count, self.title))
            time.sleep(0.2)

            video = {
                'title': self.title, 'link': self.href
            }

            self.list_video['video{0}'.format(count)] = video

            count += 1
            if count == 11:
                break

        print("{0} 비디오 DB 입력 중".format(self.pair))

        col2.insert_one(self.list_video).inserted_id


if __name__ == '__main__':
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--incognito')
    driver = webdriver.Chrome(options=chrome_options)

    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    db2 = client.daily_crawler
    col1 = db1.music_list
    col2 = db1.youtube_list
    col3 = db2.music_list
    col4 = db2.youtube_list

    YoutubeList()

    driver.close()

