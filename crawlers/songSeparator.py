# << 곡명, 가수명 split 하는 코드 >>
# 작성자 : 정예원
#
# [ 코드 설명 ]
# [[가수명, 곡명을 split 합니다]]
# 1. newArtistList 에 song_artist_main_eng1 또는 song_artist_main_kor1 을 넣습니다.
#
#
# [코드 미완성 사항]
# 1. songCrawlerNew 와 연결되지 않았습니다.


import re
from pymongo import MongoClient
import numpy as np
import pandas as pd

from data_crawling.artist_for_nlp import list_artist_NNP


class SongSeparator:
    def __init__(self, col1):
        self.col1 = col1
        self.newArtistList = []
        self.newArtistNNPList = []
        self.read_db()

    def read_db(self):
        # list_db_music = self.col1.find({'num':{"$in":self.newSongNums}})
        # list_db_music = self.newSongList

        df_artist_nlp = pd.read_pickle("../storage/df_raw_data/df_artist_nlp.pkl")
        mongoSeparator = list(self.col1.find({}))
        # separatorList = set(list(map(lambda x: x['song_artist'], mongoSeparator))) - set(df_artist_nlp['list_artist_mc'].values)

        for x in mongoSeparator:
            print("= {} 번곡 분리 시작 =".format(x['num']))

            self.num_feat_kor, self.num_feat_eng, self.num_main_kor, \
            self.num_main_eng, self.num_sub_kor, self.num_sub_eng = 0,0,0,0,0,0
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
            self.collect_db(x)

        df_list = pd.read_pickle("../storage/df_raw_data/df_list_song_artist.pkl")
        self.newArtistList = set(self.newArtistList) - set(df_list['artist'].values.tolist())

        f = open("../storage/check_new/newArtistList.txt", 'w')
        [f.write("%s\n" % i) for i in self.newArtistList]
        f.close()

        # self.newArtistNNPList = set(self.newArtistNNPList) - set(list_artist_NNP)
        # f = open("../storage/check_new/newArtistNNPList.txt", 'w')
        # [f.write("%s\n" % i) for i in self.newArtistNNPList]
        # f.close()

        return self.newArtistList


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

    def collect_db(self, x):
        print(self.list_split)
        self.music_list['list_split'] = self.list_split

        # 한국어 제목 > 영어 제목
        # ex) 화 (Feat. 진실 Of Mad Soul Child) (Fire)
        # song_title_main_kor == 화
        # => 화 로 등록
        if "song_title_main_kor" in self.list_split.keys():
            self.col1.update_one({'num': x['num']}, {'$set': {'song_title': self.list_split["song_title_main_kor"]}})
        else:
            self.col1.update_one({'num': x['num']}, {'$set': {'song_title': self.list_split["song_title_main_eng"]}})

        # 한국어 가수명 > 영어 가수명
        # ex) Dream (Prod. by 박근태) //  수지 (SUZY), 백현 (BAEKHYUN)
        # artist_main_kor1 == 수지
        # artist_sub_eng1 == SUZY
        # artist_main_kor2 == 백현
        # artist_sub_eng2 == BAEKHYUN
        # => 수지만 등록

        if "song_artist_main_kor1" in self.list_split.keys():
            self.col1.update_one({'num': x['num']}, {'$set': {'song_artist': self.list_split["song_artist_main_kor1"]}})
            self.newArtistList = self.list_split["song_artist_main_kor1"]
        else:
            self.col1.update_one({'num': x['num']}, {'$set': {'song_artist': self.list_split["song_artist_main_eng1"]}})
            self.newArtistList = self.list_split["song_artist_main_eng1"]

        # self.newArtistNNPList


from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db1 = client.music_cow
db2 = client.article
col1 = db1.musicCowData

SongSeparator(col1)