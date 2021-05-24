import re
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

class GenieList:
    def __init__(self, num):
        self.num = num
        self.read_db()

    def read_db(self):
        list_db_music = col1.find({}, {'num': {"$slice": [self.num, 1]}})
        for x in list_db_music:
            self.song_artist = x['song_artist']
            self.song_title = x['song_title']
            self.pair = self.song_artist + ' ' + self.song_title

            self.listing_genie()

    def listing_genie(self):
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"}
        self.page = 'https://www.genie.co.kr/search/searchSong?query={0}&Coll='.format(self.pair)
        self.url = requests.get(self.page, headers=headers)
        self.soup = BeautifulSoup(self.url.text, 'html.parser')

        name = self.soup.select('tr a.title')
        num = self.soup.select('a.btn-info')
        count = 0

        self.list_genie = {
            'num': self.num,
            'song_title': self.song_title,
            'song_artist': self.song_artist,
        }

        for n, i in enumerate(name):
            if self.song_artist.lower().replace(' ', '') in i.text.lower().replace(' ', ''):
                count += 1

                self.url_detail = num[n].attrs['onclick']
                self.link_genie = 'https://www.genie.co.kr/detail/songInfo?xgnm='+re.findall('\d+', self.url_detail)[0]
                print("{0}의 {1}th - {2}".format(self.pair, count, self.link_genie))

                song_info = {
                    'link': self.link_genie
                }

                self.list_genie['song_info{0}'.format(count)] = song_info

        col2.insert_one(self.list_genie).inserted_id


if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    db2 = client.daily_crawler
    col1 = db1.music_list
    col2 = db1.genie_list
    col3 = db2.music_list
    col4 = db2.genie_list

    num_music = col1.count_documents({})

    for num in range(1, num_music + 1):
        GenieList(num)