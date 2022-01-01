from googleapiclient.discovery import build
from pymongo import MongoClient
from datetime import datetime
import winsound
import json


# return 값 반환하는 형태로 대규모 개편하기


class YoutubeDailyCrawler:
    def __init__(self):
        self.read_db()

    def read_db(self):
        list_db_music = col1.find({}, {'num': {"$slice": [1, 1]}})
        for x in list_db_music:
            for num_video_order in range(1, len(x)-3):
                self.link_video = x['video{0}'.format(num_video_order)]['link']
                self.id_video = self.link_video.split('watch?v=')[1]
                self.video_num = x['video{0}'.format(num_video_order)]['video_num']
                self.title_video = x['video{0}'.format(num_video_order)]['title']
                self.song_artist = x['song_artist']
                self.song_title = x['song_title']

                if self.video_num > 0:
                    if self.video_num in [17, 21, 77, 159, 161, 176, 180, 188, 314, 319, 358, 464, 539, 596, 597, 600, 632, 683, 690, 696, 709, 789, 813, 844, 878, 914, 931, 960,
                                          1016, 1019, 1112, 1130, 1204, 1417, 1494, 1506, 1510, 1561, 1586, 1655, 1657, 1674, 1747, 1845, 1974, 2120, 2314, 2413, 2514,
                                          2671, 2702, 2764, 2826, 2849, 2889, 2899, 2934, 2981, 3007, 3033, 3046, 3067, 3074, 3088, 3130,
                                          3158, 3211, 3249, 3260, 3328, 3595, 3702, 3706, 3812, 3876, 3879, 3905, 3923, 3995, 4008, 4056, 4108, 4148, 4174, 4178,
                                          4184, 4210, 4256, 4288, 4378, 4382, 4400, 4440, 4482, 4485, 4499, 4509, 4538, 4866, 4874, 4878, 4882, 4892, 4945, 5038,
                                          5133, 5151, 5164, 5221, 5224, 5286, 5305, 5433, 5463, 5477, 5649, 5710, 5758, 5766, 5804, 5809, 5825, 5933, 5939, 6015, 6039, 6059, 6252,
                                          6286, 6306, 6367, 6368, 6384, 6442, 6480, 6561, 6588, 6613, 6653, 6700, 6729, 6772, 7079, 7086, 7098, 7102, 7120, 7174, 7184, 7216, 7290, 7352, 7363, 7367,
                                          7369, 7394, 7510, 7660, 7669, 7685, 7734, 7824, 7915, 7952, 7989, 8034, 8098, 8129, 8162, 8170, 8192]:
                        # num = 8304까지
                        # 314, 690, 6613
                        pass
                    else:
                        self.crawling_daily()
                else:
                    pass

    def crawling_daily(self):
        with open("../storage/key.json", "r") as key:
            key_dict = json.load(key)
        api_key = key_dict["google_api"]
        video_id = self.id_video

        api_obj = build('youtube', 'v3', developerKey=api_key)
        response = api_obj.videos().list(part='statistics', id=video_id, maxResults=100).execute()
        date_today = datetime.now().strftime('%Y-%m-%d')

        if response['items'] == []:
            winsound.Beep(2000, 1000)
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
                print(self.video_num, daily_crawl)

                col2.insert_one(video_info).inserted_id
                return video_info


if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    db2 = client.daily_crawler
    col1 = db1.youtube_list
    col2 = db2.daily_youtube

    YoutubeDailyCrawler()

