import re
from pymongo import MongoClient

class SongSpliter:
    def __init__(self):
        self.read_db()


    def read_db(self):
        list_db_music = col1.find({}, {'num': {"$slice": [1, 1]}})
        for x in list_db_music:
            self.num = x['num']
            if self.num < 0:
                pass
            else:
                self.num_feat_kor, self.num_feat_eng, self.num_main_kor, \
                self.num_main_eng, self.num_sub_kor, self.num_sub_eng = 0, 0, 0, 0, 0, 0
                self.music_list = x
                self.list_split = {}
                self.song_artist = x['song_artist']
                self.song_artist = self.song_artist.replace('(', ', (')
                temp_art0 = re.search('\(([^)]+)', self.song_artist)
                if temp_art0 != None:
                    temp_art1 = re.search('\(([^)]+)', self.song_artist).regs[0]
                    temp_art2 = self.song_artist[temp_art1[0]:temp_art1[1]].replace(',', '#')
                    self.song_artist = re.sub('\(([^)]+)', temp_art2, self.song_artist).split(',')
                else:
                    self.song_artist = list(self.song_artist.split(','))

                # 괄호 안 만 컴마 있어도 split 영향 안 받게??
                self.song_title = x['song_title']
                self.song_title = self.song_title.replace('(', ', (')
                temp_tit0 = re.search('\(([^)]+)', self.song_title)
                if temp_tit0 != None:
                    temp_tit1 = re.search('\(([^)]+)', self.song_title).regs[0]
                    temp_tit2 = self.song_title[temp_tit1[0]:temp_tit1[1]].replace(',', '#')
                    self.song_title = re.sub('\(([^)]+)', temp_tit2, self.song_title).split(',')
                else:
                    self.song_title = list(self.song_title.split(','))

                self.spliting_song()
                self.collect_db()

    def spliting_song(self):
        for title in self.song_title:
            if '(' in title:
                song_title_sub = re.sub('|\(|\)', '', title).replace('#', ',').strip()
                if song_title_sub[0].encode().isalpha():
                    if ('Feat.' or 'feat.') in song_title_sub:
                        self.song_artist_sub_feat = song_title_sub.replace(('Feat.' or 'feat.'), '').strip()
                        self.feat_split()
                    elif ('Prod.' or 'prod.' or 'PROD.') in song_title_sub:
                        self.song_artist_sub_feat = song_title_sub.replace(('Prod.' or 'prod.' or 'PROD.'), '').replace('by', '').strip()
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
                song_artist_sub = re.sub('|\(|\)', '', artist).replace('#', ',').strip()
                if song_artist_sub[0].encode().isalpha():
                    self.num_sub_eng += 1
                    self.list_split['song_artist_sub_eng{0}'.format(self.num_sub_eng)] = song_artist_sub.strip()
                else:
                    self.num_sub_kor += 1
                    self.list_split['song_artist_sub_kor{0}'.format(self.num_sub_kor)] = song_artist_sub.strip()
            else:
                song_artist_main = re.sub('|\(|\)', '', artist).strip()
                if song_artist_main[0].encode().isalpha():
                    self.num_main_eng += 1
                    self.list_split['song_artist_main_eng{0}'.format(self.num_main_eng)] = song_artist_main
                else:
                    self.num_main_kor += 1
                    self.list_split['song_artist_main_kor{0}'.format(self.num_main_kor)] = song_artist_main

    def feat_split(self):
        self.song_artist_sub_feat = self.song_artist_sub_feat.split(',')
        for feats in self.song_artist_sub_feat:
            if feats[0].encode().isalpha():
                self.num_feat_eng += 1
                self.list_split['song_artist_feat_eng{0}'.format(self.num_feat_eng)] = feats
            else:
                self.num_feat_kor += 1
                self.list_split['song_artist_feat_kor{0}'.format(self.num_feat_kor)] = feats

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