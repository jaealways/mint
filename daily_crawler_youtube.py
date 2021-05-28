from googleapiclient.discovery import build
from pymongo import MongoClient
from datetime import datetime

# return 값 반환하는 형태로 대규모 개편하기

class YoutubeDailyCrawler:
    def __init__(self, num):
        self.num = num
        self.read_db()

    def read_db(self):
        list_db_music = col1.find({}, {'num': {"$slice": [self.num, 1]}})
        for x in list_db_music:
            for num_video_order in range(1, 11):
                self.link_video = x['video{0}'.format(num_video_order)]['link']
                self.id_video = self.link_video.split('watch?v=')[1]
                self.title_video = x['video{0}'.format(num_video_order)]['title']
                self.song_artist = x['song_artist']
                self.song_title = x['song_title']

                self.crawling_daily()

    def crawling_daily(self):
        f = open("key.txt", 'r')
        api_key = f.read()
        video_id = self.id_video

        api_obj = build('youtube', 'v3', developerKey=api_key)
        response = api_obj.videos().list(part='statistics', id=video_id).execute()
        date_today = datetime.now().strftime('%Y-%m-%d')
        video_info = {
            'title_video': self.title_video,
            'id_video': self.id_video,
            'song_title': self.song_title,
            'song_artist': self.song_artist
        }

        while response:
            for item in response['items']:
                daily_crawl = item['statistics']
                info_list = {
                    'view': daily_crawl['viewCount'],
                    'like': daily_crawl['likeCount'],
                    'dislike': daily_crawl['dislikeCount'],
                    'comments': daily_crawl['commentCount']
                }

                video_info['{0}'.format(date_today)] = info_list
                print(info_list)

                col2.insert_one(video_info).inserted_id
                return video_info



if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db = client.music_cow
    col1 = db.youtube_list
    col2 = db.daily_youtube

    num_youtube = col1.count_documents({})

    for num in range(1, num_youtube + 1):
        YoutubeDailyCrawler(num)