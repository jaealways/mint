from datetime import datetime
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import re

class GenieDailyCrawler:
    def __init__(self, num):
        self.num = num
        self.read_db()

    def read_db(self):
        list_db_music = col1.find({}, {'num': {"$slice": [self.num, 1]}})
        for x in list_db_music:
            num_list_genie = len(x) - 4
            for y in range(1, num_list_genie):
                self.link_genie = x['song_info{0}'.format(y)]['link']
                self.song_artist = x['song_artist']
                self.song_title = x['song_title']

                self.crawling_daily()

    def crawling_daily(self):
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"}
        self.url = requests.get(self.link_genie, headers=headers)
        self.soup = BeautifulSoup(self.url.text, 'html.parser')

        song_name = str(self.soup.select('h2.name'))
        song_name = re.sub('\<.+?>|\[|\'|\]', '', song_name, 0).strip()
        album_name = str(self.soup.select('div.info-zone > ul > li:nth-child(1) > span.value > a'))
        album_name = re.sub('\<.+?>|\[|\'|\]', '', album_name, 0).strip()
        #artist_name, genre_name 빼고는 다 출력
        artist_name = str(self.soup.select('#body-content > div.song-main-infos > div.info-zone > ul > li:nth-child(2) > span.value > a'))
        artist_name = re.sub('\<.+?>|\[|\'|\]', '', artist_name, 0).strip()
        genre_name = str(self.soup.select('#body-content > div.song-main-infos > div.info-zone > ul > li:nth-child(3) > span.value'))
        genre_name = re.sub('\<.+?>|\[|\'|\]', '', genre_name, 0).strip()

        total_listener = self.soup.select('div.total p')[0].text.replace(',','')
        total_play = self.soup.select('div.total p')[1].text.replace(',','')
        like = self.soup.select_one('em#emLikeCount').text.replace(',','')

        # song_title은 원곡, song_name은 genie에 검색되는 여러 곡 중 하나의 이름
        self.list_genie = {
            'link': self.link_genie,
            'song_title': self.song_title,
            'song_artist': self.song_artist,
            'song_name': song_name, 'album_name': album_name,
            'artist_name': artist_name, 'genre_name': genre_name
        }

        date_today = datetime.now().strftime('%Y-%m-%d')

        daily_genie_list = {
            'total_listener': total_listener, 'total_play': total_play, 'like': like
        }

        self.list_genie['{0}'.format(date_today)] = daily_genie_list
        print(daily_genie_list)

        col2.insert_one(self.list_genie).inserted_id


if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    col1 = db1.genie_list
    col2 = db1.daily_genie

    num_music = col1.count_documents({})

    for num in range(1, num_music + 1):
        GenieDailyCrawler(num)