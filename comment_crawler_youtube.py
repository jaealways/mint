from googleapiclient.discovery import build
from pymongo import MongoClient

class YoutubeDailyCrawler:
    def __init__(self):
        self.read_db()

    def read_db(self):
        list_db_music = col1.find({}, {'num': {"$slice": [1, 1]}})
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
        response = api_obj.commentThreads().list(part='snippet,replies', videoId=video_id).execute()
        list_comment = {
            'title_video': self.title_video,
            'id_video': self.id_video,
            'song_title': self.song_title,
            'song_artist': self.song_artist
        }


        while response:
            count = 1
            for item in response['items']:
                comment = item['snippet']['topLevelComment']['snippet']
                comments = {
                    'comment': comment['textDisplay'],
                    'author': comment['authorDisplayName'],
                    'date': comment['publishedAt'],
                    'like': comment['likeCount']
                }

                if item['snippet']['totalReplyCount'] > 0:
                    for reply_item in item['replies']['comments']:
                        reply = reply_item['snippet']
                        comments = {
                            'comment': reply['textDisplay'],
                            'author': comment['authorDisplayName'],
                            'date': comment['publishedAt'],
                            'like': comment['likeCount']
                        }

                list_comment['comment{0}'.format(count)] = comments
                print(comments)
                count += 1

            if 'nextPageToken' in response:
                response = api_obj.commentThreads().list(part='snippet,replies', videoId=video_id, pageToken=response['nextPageToken'], maxResults=100).execute()
            else:
                break
        col2.insert_one(list_comment).inserted_id


if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db = client.music_cow
    col1 = db.youtube_list
    col2 = db.daily_youtube

    YoutubeDailyCrawler()
