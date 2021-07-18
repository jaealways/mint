from pymongo import MongoClient
import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from bson.json_util import dumps, loads
import subprocess as cmd

class MusicCowPriceRatio:
    def __init__(self):
        #self.date_today = datetime.strptime('2021-07-13', "%Y-%m-%d").date()
        #self.date_today = datetime.now().strftime('%Y-%m-%d')
        self.price_convert_to_ratio()

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


        a = df.corr()

        for i in range(0,783):
            print('')
            print('')
            print('==================================================')
            for j in range(0,783):
                if j==i:
                    continue
                elif abs(a.iloc[j,i]) >= 0.7:
                    print('{0}번 <-> {1}번'.format(a.columns[i],a.index[j]))
                    print(a.iloc[j,i])
                else:
                    pass


        # print(a)


        # sns.heatmap(a, annot=True)

        # sns.heatmap(df, annot=True)
        # plt.title('heat map',fontsize = 20)
        # plt.show()


if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    db2 = client.daily_crawler
    # music cow
    col1 = db1.daily_music_cow
    col2 = db1.music_cow_ratio

    MusicCowPriceRatio()

