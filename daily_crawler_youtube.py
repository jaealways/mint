from googleapiclient.discovery import build
from pymongo import MongoClient
from datetime import datetime

# return 값 반환하는 형태로 대규모 개편하기

class YoutubeDailyCrawler:
    def __init__(self):
        self.read_db()

    def read_db(self):
        list_db_music = col1.find({}, {'num': {"$slice": [1, 1]}})
        for x in list_db_music:
            for num_video_order in range(1, 11):
                self.link_video = x['video{0}'.format(num_video_order)]['link']
                self.id_video = self.link_video.split('watch?v=')[1]
                self.video_num = x['video{0}'.format(num_video_order)]['video_num']
                self.title_video = x['video{0}'.format(num_video_order)]['title']
                self.song_artist = x['song_artist']
                self.song_title = x['song_title']

                if self.video_num > 0:
                    self.crawling_daily()
                else:
                    pass

    def crawling_daily(self):
        f = open("key.txt", 'r')
        api_key = f.read()
        video_id = self.id_video

        api_obj = build('youtube', 'v3', developerKey=api_key)
        response = api_obj.videos().list(part='statistics', id=video_id, maxResults=100).execute()
        date_today = datetime.now().strftime('%Y-%m-%d')
        if response['items'] == []:
            #에러뜨면 카톡오게, 에러뜨면 자동으로 삭제하고 다시
            print('{0}번 오류 발생 - {1}'.format(self.video_num, self.id_video))
            print('{0} {1}'.format(self.song_artist, self.song_title))
            raise IndexError
        video_info = {
            'title_video': self.title_video,
            'id_video': self.id_video,
            'video_num': self.video_num,
            'song_title': self.song_title,
            'song_artist': self.song_artist
        }

        while response:
            for item in response['items']:
                daily_crawl = item['statistics']

                video_info['{0}'.format(date_today)] = daily_crawl
                print(daily_crawl)

                col2.insert_one(video_info).inserted_id
                return video_info



if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    db2 = client.daily_crawler
    col1 = db1.youtube_list
    col2 = db2.daily_youtube

    YoutubeDailyCrawler()