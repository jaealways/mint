from googleapiclient.discovery import build
from pymongo import MongoClient

class YoutubeDailyCrawler:
    def __init__(self):
        self.read_db()

    def read_db(self):
        list_db_music = col1.find({})
        for x in list_db_music:

            for num_video_order in range(1, len(x)-4):
                self.video_num = x['video{0}'.format(num_video_order)]['video_num']
                # self.num_video += 1
                self.link_video = x['video{0}'.format(num_video_order)]['link']
                self.id_video = self.link_video.split('watch?v=')[1]
                self.title_video = x['video{0}'.format(num_video_order)]['title']
                self.song_artist = x['song_artist']
                self.song_title = x['song_title']

                if self.video_num < 273:
                    continue
                if self.video_num in [271, 269, 268, 267, 266, 8180, 265, 264, 252, 251, 243, 234, 233, 223, 213, 201, 199, 192, 183, 142, 24, 132, 129, 123, 112, 105, 103, 102, 99, 97, 96, 81, 5, 7, 17, 25, 33, 42, 44, 52, 61, 66, 73, 159, 161, 176, 180, 188, 319, 358, 464, 539, 596, 597, 600, 632, 683, 696,
                                      709, 789, 813, 878, 914, 931,
                                      1016, 1019, 1112, 1130, 1204, 1417, 1494, 1506, 1510, 1561, 1586, 1655, 1657,
                                      1674, 1747, 1845, 1974, 2120, 2314, 2413, 2514,
                                      2671, 2764, 2826, 2849, 2889, 2899, 2934, 2981, 3007, 3033, 3046, 3067, 3074,
                                      3088, 3130,
                                      3158, 3211, 3249, 3260, 3328, 3595, 3702, 3706, 3812, 3879, 3905, 3923, 3995,
                                      4008, 4056, 4108, 4148, 4174, 4178,
                                      4184, 4210, 4256, 4288, 4378, 4382, 4440, 4482, 4485, 4499, 4509, 4538, 4866,
                                      4874, 4882, 4892, 4945, 5038,
                                      5133, 5151, 5164, 5221, 5224, 5286, 5305, 5433, 5463, 5477, 5649, 5710, 5758,
                                      5766, 5804, 5809, 5825, 5933, 5939, 6015, 6039, 6059, 6252,
                                      6286, 6306, 6367, 6368, 6384, 6442, 6480, 6561, 6588, 6653, 6700, 6729, 7079,
                                      7098, 7102, 7120, 7184, 7216, 7290, 7352, 7363,
                                      7369, 7394, 7510, 7660, 7685, 7734, 7824, 7915, 7952, 7989, 8034, 8098, 8162,
                                      8170]:
                    # num = 8290까지
                    continue
                else:
                    self.crawling_daily()

    def crawling_daily(self):
        f = open("../key.txt", 'r')
        api_key = f.read()
        video_id = self.id_video
        print('이번 비디오는 :', self.video_num)

        api_obj = build('youtube', 'v3', developerKey=api_key)
        try:
            response = api_obj.commentThreads().list(part='snippet,replies', videoId=video_id).execute()
            while response:
                list_comment = {}
                list_comment = {
                    'video_num': self.video_num,
                    'title_video': self.title_video,
                    'id_video': self.id_video,
                    'song_title': self.song_title,
                    'song_artist': self.song_artist
                }
                count = 1
                for item in response['items']:
                    comment = item['snippet']['topLevelComment']['snippet']
                    comments = {
                        'comment': comment['textDisplay'],
                        'author': comment['authorDisplayName'],
                        'date': comment['publishedAt'].split('T')[0],
                        'like': comment['likeCount']
                    }

                    if item['snippet']['totalReplyCount'] > 0:
                        for reply_item in item['replies']['comments']:
                            reply = reply_item['snippet']
                            comments = {
                                'comment': reply['textDisplay'],
                                'author': comment['authorDisplayName'],
                                'date': comment['publishedAt'].split('T')[0],
                                'like': comment['likeCount']
                            }

                    list_comment['comment{0}'.format(count)] = comments
                    print(comments)
                    count += 1
                    if count == 100:
                        col2.insert_one(list_comment).inserted_id

                if 'nextPageToken' in response:
                    response = api_obj.commentThreads().list(part='snippet,replies', videoId=video_id, pageToken=response['nextPageToken'], maxResults=4000).execute()
                else:
                    break
            col2.insert_one(list_comment).inserted_id
        finally:
            pass



if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    db2 = client.daily_crawler
    col1 = db1.youtube_list
    col2 = db2.comment_youtube

    YoutubeDailyCrawler()

