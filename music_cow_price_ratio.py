from pymongo import MongoClient
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from bson.json_util import dumps, loads
import subprocess as cmd

class MusicCowPriceRatio:
    def __init__(self):
        #self.date_today = datetime.strptime('2021-07-13', "%Y-%m-%d").date()
        #self.date_today = datetime.now().strftime('%Y-%m-%d')
        self.related_songs()
        self.price_convert_to_ratio()
        self.compare_corr_to_related()


    def price_convert_to_ratio(self):
        list_db_gen_daily = col1.find({}, {'num': {"$slice": [1, 1]}})
        df = pd.DataFrame()

        for x in list_db_gen_daily:
            if x['num'] == 1388:   #### 임시 data 개수 맞지 않아 dataframe 으로 matching 안됨.
                break

            #print('')
            #print(x['num'])
            #print('')     ## x -> 의 index 4번째 index 가 첫 곡이 된다 => idea

            ratio = []

            for price in range(4,len(x)-1):
                #print(list(x.values())[price].get('price'))
                ratio.append(int(list(x.values())[price].get('price')))

                self.rate_of_change = ((int(list(x.values())[price+1].get('price')) - int(list(x.values())[price].get('price'))) / int(list(x.values())[price].get('price'))) * 100

                #print(self.rate_of_change)

                #col2.update_one({'num': x['num']}, {'$set': {list(x.keys())[price]: self.rate_of_change}}, upsert=True)

            df[x['num']] = ratio

        a = df.corr()  # 상관계수 데이터프레임

        self.corr = {}  #'함께구매한 곡과 비교하기 위한 상관계수 =>0.7 인 곡들 딕셔너리

        for i in range(0,783):
            #print('')
            #print('')
            #print('==================================================')
            self.corr[a.columns[i]] = []
            for j in range(0,783):
                if j==i:
                    continue
                elif abs(a.iloc[j,i]) >= 0.7:
                    #print('{0}번 <-> {1}번'.format(a.columns[i],a.index[j]))
                    self.corr[a.columns[i]].append(a.index[j])
                    #print(a.iloc[j,i])
                else:
                    pass



        # print(a)


        # sns.heatmap(a, annot=True)

        # sns.heatmap(df, annot=True)
        # plt.title('heat map',fontsize = 20)
        # plt.show()



    def related_songs(self):

        list_db_gen_daily = col1.find({}, {'num': {"$slice": [1, 1]}})
        self.related_songs = {}

        for x in list_db_gen_daily:
            if x['num'] == 1388:  #### 임시 data 개수 맞지 않아 dataframe 으로 matching 안됨.
                break

            # '함께 구매한 곡' 크롤링
            url = "https://www.musicow.com/song/{0}?tab=info".format(x['num'])
            resp = requests.get(url)
            html = resp.text

            soup = BeautifulSoup(html, 'html.parser')



            links = soup.find_all("a", attrs={"class": "thmb"})

            self.related_songs[x['num']] = []

            for a in links:
                href = a.attrs['href'][6:]
                self.related_songs[x['num']].append(href)

            # related_song = list(soup.select(
            #      "#page_market > div.container > div.song_tab.tab_info.on > section:nth-child(2) > div.card_body > div > div.lst_rcmd.swiper-wrapper  a:nth-child(2)").attrs['href'])
            # # for songs in related_song:
            #     type(songs)

            # related_songs[x['num']] = list(related_song)

    def compare_corr_to_related(self):

        for num1, num2 in self.related_songs, self.corr:
            for song1 in num1:
                for song2 in num2:
                    if song1 == song2:
                        print(song1)
                    else:
                        pass


if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    db2 = client.daily_crawler
    # music cow
    col1 = db1.daily_music_cow
    col2 = db1.music_cow_ratio

    MusicCowPriceRatio()