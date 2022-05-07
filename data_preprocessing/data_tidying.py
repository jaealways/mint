from datetime import datetime
import numpy as np

from data_transformation.db_env import DbEnv, db

import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm


class DataTidying:
    """모든 데이터프레임 생성시 column: date, row: num으로 되게 통일하기"""

    def get_list_from_sql(self, cursor, sql):
        list_music_num = DbEnv().get_data_from_table(cursor, sql)
        list_music_num = [item[0] for item in list_music_num]

        return list_music_num

    def get_df_price(self, date, list_num):
        df_price = pd.DataFrame()

        for num in tqdm(list_num):
            sql = "SELECT DISTINCT date, price_close FROM musiccowdata WHERE num='%s' and date>='%s' ORDER BY date" % (num, date)
            df_temp = db(cursor, sql).dataframe
            df_temp = df_temp.set_index('date')
            df_temp.columns = [num]

            df_price = pd.concat([df_price, df_temp], axis=1)

        return df_price.T

    def get_df_mcpi(self, date):
        sql = "SELECT DISTINCT date, price FROM dailymcpi WHERE date>='%s' ORDER BY date" % date
        df_mcpi = db(cursor, sql).dataframe

        df_mcpi = df_mcpi.set_index('date')
        df_mcpi.columns = [0]

        df_mcpi = np.transpose(df_mcpi)

        return df_mcpi

    def get_list_song_artist(self, cursor):
        sql = "SELECT DISTINCT num, title, artist FROM list_song_artist ORDER BY num"
        df_list = db(cursor, sql).dataframe

        df_list = df_list.set_index('num')
        df_list.to_pickle("../storage/df_raw_data/df_list_song_artist.pkl")

        return df_list

    def get_df_song_volume(self, cursor, str_date='17-01-01', end_date='23-12-31'):
        df_song_volume = pd.DataFrame()
        df_mcpi = pd.read_pickle("../storage/df_raw_data/df_mcpi_17-01-01_23-12-31.pkl")
        list_mcpi_date = df_mcpi.loc[:, '20' + str_date: '20' + end_date].columns.tolist()
        for date in tqdm(list_mcpi_date):
            sql = "SELECT DISTINCT num, volume FROM daily_music_cow WHERE date='%s' ORDER BY num" % (date)
            df_temp = db(cursor, sql).dataframe
            df_temp = df_temp.set_index('num')
            df_temp.columns = ["%s" % date]

            df_song_volume = pd.concat([df_song_volume, df_temp], axis=1)

        df_song_volume.to_pickle("../storage/df_raw_data/df_song_volume_%s_%s.pkl" % (str_date, end_date))

        return df_song_volume

    def get_df_fng_index(self, str_date='17-01-01', end_date='23-12-31'):
        df_mcpi = pd.read_pickle("../../fear-and-greed/df_mcpi_17-01-01_23-12-31.pkl")
        df_song_volume = pd.read_pickle("../storage/df_raw_data/df_song_volume_17-01-01_23-12-31.pkl")

        df_mcpi_volume = np.transpose(pd.DataFrame(df_song_volume.sum()))
        df_fng_index = pd.concat([df_mcpi, df_mcpi_volume], axis=0)
        df_fng_index.index = ['mcpi', 'total_volume']

        df_mcpi_volume.to_pickle("../storage/df_raw_data/df_mcpi_volume_%s_%s.pkl" % (str_date, end_date))
        df_fng_index.to_pickle("../storage/df_raw_data/df_fng_index_%s_%s.pkl" % (str_date, end_date))

        return df_mcpi_volume, df_fng_index

    def get_df_copyright(self):
        # 곡 출시되기 이전 정보는 nan 처리하는 로직 만들기
        # copyright_prices = pd.read_pickle("../storage/df_raw_data/copyright_prices.pkl")
        list_copyright = list(col3.find({}))
        df_copyright_prices = pd.DataFrame([])

        for dict_copy in list_copyright:
            dict_val = {}
            temp = '0'
            for key, val in dict_copy.items():
                if key == 'num':
                    num = val
                    continue
                elif key == '_id':
                    continue
                else:
                    if val == '0':
                        if temp == '0':
                            val = np.nan
                        elif key == DYToday:
                            val = np.nan
                        else:
                            val = int(val)
                            temp = val
                    else:
                        val = val.replace(',', '')
                        val = int(val)
                dict_val[key] = val
            df_temp = pd.DataFrame.from_dict(dict_val, orient='index').rename(columns={0: num}).T
            df_copyright_prices = pd.concat([df_copyright_prices, df_temp])

        return df_copyright_prices

    def get_df_song_genre(self):
        df_genre = pd.DataFrame()

        client = MongoClient('localhost', 27017)
        col = client['music_cow']['genre']
        list_db = col.find({})
        for x in list_db:
            song_num, genre = x['num'], x['genre']
            genre = genre.split('/')[-1].replace(' ', '')
            df_temp = pd.DataFrame([song_num, genre]).T
            df_genre = pd.concat([df_genre, df_temp])

        df_genre.columns = ['num', 'genre']
        df_genre = df_genre.set_index('num')
        df_genre.to_pickle("../storage/df_raw_data/df_genre.pkl")

        return df_genre


conn, cursor = DbEnv().connect_sql()
client = MongoClient('localhost', 27017)
db1 = client.music_cow
col3 = db1.copyright_price

DYToday = datetime.today().strftime("%Y-%m")

# DataTidying().get_df_song_genre()
# list_music_num = DataTidying().get_list_song_artist(cursor)

if __name__ == '__main__':
    DataTidying().get_df_copyright()

