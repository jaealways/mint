from pymongo import MongoClient
import pandas as pd
import numpy as np

class CopyrightPriceYearly:
    def __init__(self):
        self.copyright_yearly()
        self.copyright_yearly_colmeans()

    def copyright_yearly(self):
        list_db_gen_daily = col4.find({}, {'num': {"$slice": [1, 1]}})
        music_list = col3.find({}, {'num': {"$slice": [1, 1]}})
        music_list_split = col2.find({}, {'num': {"$slice": [1, 1]}})

        stock_num={}
        for i in music_list:
            stock_num[i['num']] = int(i['stock_num'])

        song_released_date = {}
        for j in music_list_split:
            song_released_date[j['num']] = int(j['song_release_date'][0:4])

        copyright_yearly = pd.DataFrame([])

        for x in list_db_gen_daily:
            prices = {}
            prices['발매년도'] = song_released_date[x['num']]
            for a in range(2017,2022):
                yearly_price = 0
                for b in range(1,13):
                    price = x['{0}-{1}'.format(a, b)]
                    if ',' in price:
                        price = price.replace(",", "")
                    yearly_price = yearly_price + int(price)
                prices[a] = yearly_price * stock_num[x['num']]


            for c in range(2017, 2021):
                if prices[c]>0:
                    prices[c+1-song_released_date[x['num']]] = ((prices[c+1] - prices[c]) / prices[c]) * 100
                else:
                    continue

            copyright_yearly = copyright_yearly.append(prices, ignore_index=True)

        num = 6
        #copyright_yearly.to_pickle('df_copyright_yearly.pkl')
        print(copyright_yearly)


    def copyright_yearly_colmeans(self):
        df = pd.read_pickle('df_copyright_yearly.pkl')

        data = df.iloc[:,6:]
        data = data.sort_index(axis=1)
        colname = data.columns.tolist()


        for i in range(0,34):
            print('{0}년차   | 평균: {1}, 표준편차 {2}   - {3} 개'.format(colname[i], np.mean(data.iloc[:,i]), np.std(data.iloc[:,i]), sum(~np.isnan(data.iloc[:, i]))))






if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    db2 = client.daily_crawler
    # music cow
    col1 = db1.daily_music_cow
    col2 = db1.music_list_split
    col3 = db1.music_list
    col4 = db1.copyright_price

    CopyrightPriceYearly()