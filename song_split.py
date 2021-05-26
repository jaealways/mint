import re
from pymongo import MongoClient

class SongSpliter:
    def __init__(self, num):
        self.num = num
        self.read_db()

    def read_db(self):
        list_db_music = col1.find({}, {'num': {"$slice": [self.num, 1]}})
        for x in list_db_music:
            self.song_artist = x['song_artist']
            self.song_artist = self.song_artist.replace('(', ', (').split(',')
            self.song_title = x['song_title']
            self.song_title = self.song_title.replace('(', ', (').split(',')
            self.song_title_main_eng = ''
            self.song_title_main_kor = ''
            self.song_title_sub_eng = ''
            self.song_title_sub_kor = ''
            self.song_artist_main_eng = ''
            self.song_artist_main_kor = ''
            self.song_artist_sub_eng = ''
            self.song_artist_sub_kor = ''
            self.song_artist_feat_eng = ''
            self.song_artist_feat_kor = ''
            self.song_artist_sub_feat = ''

            self.spliting_song()
            self.collect_db()

    def spliting_song(self):
        for title in self.song_title:
            self.song_title_main = re.sub('|\(|\)', '', title).strip()
            if self.song_title_main[0].encode().isalpha():
                self.song_title_main_eng = self.song_title_main
            else:
                self.song_title_main_kor = self.song_title_main

            self.song_title_sub = re.findall('\(([^)]+)', title)
            for feat in self.song_title_sub:
                feat = feat.strip()
                if feat[0].encode().isalpha():
                    if 'Feat.' or 'feat.' in feat:
                        self.song_artist_sub_feat = feat.replace('Feat.' or 'feat.', '').strip()
                        self.feat_split()
                    elif 'Prod.' or 'prod.' or 'PROD.' in feat:
                        self.song_artist_sub_feat = feat.replace('Prod.' or 'prod.' or 'PROD.', '').strip()
                        self.feat_split()
                    self.song_title_sub_eng = feat.strip()
                else:
                    self.song_title_sub_kor = feat.strip()

        for artist in self.song_artist:
            self.song_artist_main = re.sub('|\(|\)', '', artist).strip()
            if self.song_artist_main[0].encode().isalpha():
                self.song_artist_main_eng = self.song_artist_main
            else:
                self.song_artist_main_kor = self.song_artist_main

            self.song_artist_sub = re.findall('\(([^)]+)', artist)
            for sub in self.song_artist_sub:
                sub = sub.strip()
                if sub.encode().isalpha():
                    self.song_artist_sub_eng = sub
                else:
                    self.song_artist_sub_kor = sub

    def feat_split(self):
        for sub_feat in self.song_artist_sub_feat:
            sub_feat = sub_feat.strip()
            if sub_feat[0].encode().isalpha():
                self.song_artist_feat_eng = sub_feat
            else:
                self.song_artist_feat_kor = sub_feat

    def collect_db(self):
        self.list_split = {
            'song_title':{
                'main':{
                    'eng': self.song_title_main_eng, 'kor': self.song_title_main_kor},
                'sub':{
                    'eng': self.song_title_sub_eng, 'kor': self.song_title_sub_kor},
                    },
            'song_artist':{
                'main':{
                    'eng': self.song_artist_main_eng, 'kor': self.song_artist_main_kor},
                'sub':{
                    'eng': self.song_artist_sub_eng, 'kor': self.song_artist_sub_kor},
                'feat':{
                    'eng': self.song_artist_feat_eng, 'kor': self.song_artist_feat_kor}
                }
            }

        print(self.list_split)


if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    col1 = db1.music_list

    num_music = col1.count_documents({})

    for num in range(1, num_music + 1):
        SongSpliter(num)
