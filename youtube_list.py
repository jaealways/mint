from bs4 import BeautifulSoup
from selenium import webdriver
from pymongo import MongoClient
import time

class YoutubeList:
    def __init__(self):
        self.video_num = 3980
        self.read_db()

    def read_db(self):
        list_db_music = col1.find({}, {'num': {"$slice": [1, 1]}})
        for x in list_db_music:
            self.num = x['num']
            if self.num < 699:
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
                self.pair = self.pair.replace('%', '%25')
                self.pair = self.pair.replace('&', '%26')


                self.listing_youtube()

    def listing_youtube(self):
        page_youtube = "https://www.youtube.com/search?q={0}&sp=EgIQAQ%253D%253D".format(self.pair)
        driver.get(page_youtube)

        html_youtube = driver.page_source
        soup_youtube = BeautifulSoup(html_youtube, 'html.parser')
        time.sleep(1)

        search_num_youtube = soup_youtube.select('a#video-title')
        count = 1
        print("{0} Youtube List 출력 중".format(self.pair))

        self.list_video = {
            'num': self.num,
            'song_title': self.song_title,
            'song_artist': self.song_artist,
        }

        for i in search_num_youtube:
            self.video_num +=1
            self.href = i.attrs['href']
            self.href = "https://youtube.com{0}".format(self.href)
            self.title = i.attrs['aria-label']
            self.title = self.title.split('게시자')[0].strip()
            print("{0}: {1}의 {2}th - {3}".format(self.video_num, self.pair, count, self.title))
            print('')
            time.sleep(0.2)

            video = {
                'title': self.title, 'link': self.href, 'video_num': self.video_num
            }

            self.list_video['video{0}'.format(count)] = video

            count += 1
            if count == 11:
                break

        print("{0} 비디오 DB 입력 중".format(self.pair))

        if self.video_num % 10 != 0:
            driver.close()
            raise IndexError

        col2.insert_one(self.list_video).inserted_id


if __name__ == '__main__':
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--incognito')
    driver = webdriver.Chrome(options=chrome_options)

    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    col1 = db1.music_list_split
    col2 = db1.youtube_list

    YoutubeList()

    driver.close()

