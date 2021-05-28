import re
from pymongo import MongoClient

class SongSpliter:
    def __init__(self):
        self.read_db()

    def read_db(self):
        list_db_music = col1.find({}, {'num': {"$slice": [1, 1]}})
        for x in list_db_music:
            self.music_list = x
            self.list_split = {}
            self.song_artist = x['song_artist']
            self.song_artist = self.song_artist.replace('(', ', (').split(',')
            self.song_title = x['song_title']
            self.song_title = self.song_title.replace('(', ', (').split(',')

            self.spliting_song()
            self.collect_db()

    def spliting_song(self):
        for title in self.song_title:
            if '(' in title:
                song_title_sub = re.sub('|\(|\)', '', title).strip()
                if song_title_sub[0].encode().isalpha():
                    if 'Feat.' or 'feat.' in song_title_sub:
                        self.song_artist_sub_feat = song_title_sub.replace('Feat.' or 'feat.', '').strip()
                        self.feat_split()
                    elif 'Prod.' or 'prod.' or 'PROD.' in song_title_sub:
                        self.song_artist_sub_feat = song_title_sub.replace('Prod.' or 'prod.' or 'PROD.', '').strip()
                        self.feat_split()
                    else:
                        self.list_split['song_title_sub_eng'] = song_title_sub.strip()
                else:
                    self.list_split['song_title_sub_kor'] = song_title_sub.strip()
            else:
                song_title_main = title.strip()
                if song_title_main[0].encode().isalpha():
                    self.list_split['song_title_main_eng'] = song_title_main
                else:
                    self.list_split['song_title_main_kor'] = song_title_main

        for artist in self.song_artist:
            if '(' in artist:
                song_artist_sub = re.sub('|\(|\)', '', artist).strip()
                if song_artist_sub[0].encode().isalpha():
                    self.list_split['song_artist_sub_eng'] = song_artist_sub.strip()
                else:
                    self.list_split['song_artist_sub_kor'] = song_artist_sub.strip()
            else:
                song_artist_main = re.sub('|\(|\)', '', artist).strip()
                if song_artist_main[0].encode().isalpha():
                    self.list_split['song_artist_main_eng'] = song_artist_main
                else:
                    self.list_split['song_artist_main_kor'] = song_artist_main

    def feat_split(self):
        if self.song_artist_sub_feat[0].encode().isalpha():
            self.list_split['song_artist_feat_eng'] = self.song_artist_sub_feat
        else:
            self.list_split['song_artist_feat_kor'] = self.song_artist_sub_feat

    def collect_db(self):
        print(self.list_split)
        self.music_list['list_split'] = self.list_split
        col2.insert_one(self.music_list).inserted_id

if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    col1 = db1.music_list
    col2 = db1.music_list_split

    SongSpliter()
