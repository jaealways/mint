from bs4 import BeautifulSoup
from selenium import webdriver
from pymongo import MongoClient
import re
import time

class YoutubeDailyCrawler:
    def __init__(self):
        self.read_db()

    def read_db(self):
        list_db_music = col1.find({}, {'num': {"$slice": [1, 1]}})
        for x in list_db_music:
            for num_video_order in range(1,11):
                self.link_video = x['video{0}'.format(num_video_order)]['link']
                self.title_video = x['video{0}'.format(num_video_order)]['title']

                self.crawling_daily()

    def crawling_daily(self):
        driver.get(self.link_video)

        html_youtube = driver.page_source
        soup_youtube = BeautifulSoup(html_youtube, 'html.parser')
        time.sleep(5)

        search_num_youtube = soup_youtube.select('#menu > ytd-menu-renderer')
        # > yt-formatted-string

        for num_like in search_num_youtube:
            num_like = num_like.attrs['aria-label']
            if '좋아요' in num_like:
                self.like = num_like
            else:
                self.dislike = num_like
        count = 1
        print("{0} Youtube 댓글 출력 중".format(self.title_video))

        self.video = {
            'title': self.title_video,
            'like': self.like,
            'dislike': self.dislike,
            'num_comment': self.num_comment
        }

        for i in search_num_youtube:
            self.comment = str(i('#content-text'))
            self.comment = re.sub('\<.+?>|\[|\'|\]', '', self.comment, 0).strip()
            self.comment_like = str(i('#vote-count-left'))
            self.comment_like = re.sub('\<.+?>|\[|\'|\]', '', self.comment_like, 0).strip()
            print(self.comment, self.comment_like)
            time.sleep(0.2)

            comment = {
                'title': self.comment, 'like': self.comment_like
            }

            self.list_video['video{0}'.format(count)] = comment


        print("{0} 비디오 댓글 DB 입력 중".format(self.title_video))

        col2.insert_one(self.list_video).inserted_id


    def collect_db(self):
        daily_youtube = {
            'song_title': self.song_title,
            'song_artist': self.song_artist,


        }

if __name__ == '__main__':
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--incognito')
    driver = webdriver.Chrome(options=chrome_options)

    client = MongoClient('localhost', 27017)
    db = client.music_cow
    col1 = db.youtube_list
    col2 = db.daily_youtube

    YoutubeDailyCrawler()

    driver.close()


