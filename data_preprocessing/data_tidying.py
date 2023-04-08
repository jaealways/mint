from datetime import datetime, timedelta
import numpy as np

from data_transformation.db_env import DbEnv, db

import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm


class DataTidying:
    def get_list_from_sql(self, cursor, sql):
        list_music_num = DbEnv().get_data_from_table(cursor, sql)
        list_music_num = [item[0] for item in list_music_num]

        return list_music_num

    def get_df_price(self, num, duration=365):
        # date_df = (datetime.today() - timedelta(days=duration+2)).strftime('%Y-%m-%d')

        # sql = "SELECT DISTINCT date, price_close FROM musiccowdata WHERE num='%s' and date>='%s' ORDER BY date" % (num, date_df)
        sql = "SELECT DISTINCT date, price_close FROM musiccowdata WHERE num='%s' ORDER BY date" % (num)

        df_temp = db(cursor, sql).dataframe
        df_temp = df_temp.set_index('date')
        df_temp.columns = [num]

        return df_temp

    def get_df_price_high(self, num, duration=365):
        # date_df = (datetime.today() - timedelta(days=duration+2)).strftime('%Y-%m-%d')

        # sql = "SELECT DISTINCT date, price_high FROM musiccowdata WHERE num='%s' and date>='%s' ORDER BY date" % (num, date_df)
        sql = "SELECT DISTINCT date, price_high FROM musiccowdata WHERE num='%s' ORDER BY date" % (num)

        df_temp = db(cursor, sql).dataframe
        df_temp = df_temp.set_index('date')
        df_temp.columns = [num]

        return df_temp

    def get_df_price_low(self, num, duration=365):
        # date_df = (datetime.today() - timedelta(days=duration+2)).strftime('%Y-%m-%d')

        # sql = "SELECT DISTINCT date, price_low FROM musiccowdata WHERE num='%s' and date>='%s' ORDER BY date" % (num, date_df)
        sql = "SELECT DISTINCT date, price_low FROM musiccowdata WHERE num='%s' ORDER BY date" % (num)

        df_temp = db(cursor, sql).dataframe
        df_temp = df_temp.set_index('date')
        df_temp.columns = [num]

        return df_temp

    def get_df_price_volume(self, num, duration=365):
        # date_df = (datetime.today() - timedelta(days=duration+2)).strftime('%Y-%m-%d')

        # sql = "SELECT DISTINCT date, volume FROM musiccowdata WHERE num='%s' and date>='%s' ORDER BY date" % (num, date_df)
        sql = "SELECT DISTINCT date, volume FROM musiccowdata WHERE num='%s'ORDER BY date" % (num)

        df_temp = db(cursor, sql).dataframe
        df_temp = df_temp.set_index('date')
        df_temp.columns = [num]

        return df_temp

    def get_df_mcpi(self):
        # sql = "SELECT DISTINCT date, price FROM dailymcpi WHERE date>='%s' ORDER BY date" % date
        sql = "SELECT DISTINCT date, price FROM dailymcpi ORDER BY date"

        df_mcpi = db(cursor, sql).dataframe

        df_mcpi = df_mcpi.set_index('date')
        df_mcpi.columns = [0]

        df_mcpi = np.transpose(df_mcpi)

        return df_mcpi

    def get_df_mcpi_volume(self):
        # sql = "SELECT DISTINCT date, volume FROM dailymcpi WHERE date>='%s' ORDER BY date" % date
        sql = "SELECT DISTINCT date, volume FROM dailymcpi ORDER BY date"

        df_mcpi_volume = db(cursor, sql).dataframe

        df_mcpi_volume = df_mcpi_volume.set_index('date')
        df_mcpi_volume.columns = [0]

        df_mcpi_volume = np.transpose(df_mcpi_volume)

        return df_mcpi_volume

    def get_list_song_artist(self, cursor):
        sql = "SELECT DISTINCT num, title, artist FROM list_song_artist ORDER BY num"
        df_list = db(cursor, sql).dataframe

        df_list = df_list.set_index('num')
        df_list.to_pickle("../storage/df_raw_data/df_list_song_artist.pkl")

        return df_list


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
                        try:
                            val = int(val)
                        except:
                            val = val
                dict_val[key] = val
            df_temp = pd.DataFrame.from_dict(dict_val, orient='index').rename(columns={0: num}).T
            df_copyright_prices = pd.concat([df_copyright_prices, df_temp])

        return df_copyright_prices

    def get_df_stock_num(self):
        list_stock = list(col4.find({}))
        df_stock = pd.DataFrame([])

        for dict_copy in list_stock:
            dict_val = {}
            for key, val in dict_copy.items():
                if key == 'num':
                    song_num = val
                elif key == 'stock_num':
                    num = int(val)
                else:
                    pass
            dict_val[song_num] = num
            df_temp = pd.DataFrame.from_dict(dict_val, orient='index')
            df_stock = pd.concat([df_stock, df_temp])

        return df_stock

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
col4 = db1.musicInfo

DYToday = datetime.today().strftime("%Y-%m")

# DataTidying().get_df_song_genre()
# list_music_num = DataTidying().get_list_song_artist(cursor)

if __name__ == '__main__':
    DataTidying().get_df_stock_num()

