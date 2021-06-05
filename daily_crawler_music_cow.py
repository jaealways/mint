from urllib.request import urlopen
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime

class MusicCowDailyCrawler:
    def __init__(self):
        self.read_db()

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
        html = urlopen("https://www.musicow.com/song/{0}?tab=price".format(self.num))
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