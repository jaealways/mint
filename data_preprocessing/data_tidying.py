import numpy as np

from data_transformation.db_env import DbEnv, db

import pandas as pd
from tqdm import tqdm


class DataTidying:
    """모든 데이터프레임 생성시 column: date, row: num으로 되게 통일하기"""

    def get_list_music_num(self, cursor):
        sql = "SELECT DISTINCT num FROM daily_music_cow"
        list_music_num = DbEnv().get_data_from_table(cursor, sql)
        list_music_num = [item[0] for item in list_music_num]

        return list_music_num

    def get_df_price(self, list_num, cursor, str_date='17-01-01', end_date='23-12-31'):
        df_price = pd.DataFrame()
        for num in tqdm(list_num):
            sql = "SELECT DISTINCT date, price_close FROM daily_music_cow WHERE (num = '%s') AND (date BETWEEN '%s' AND '%s') ORDER BY date" % (num, '20'+str_date, '20'+end_date)
            df_temp = db(cursor, sql).dataframe
            df_temp = df_temp.set_index('date')
            df_temp.columns = ["%d" % num]

            df_price = pd.concat([df_price, df_temp], axis=1)

        df_price = np.transpose(df_price)
        df_price.to_pickle("../storage/df_raw_data/df_price_%s_%s.pkl" % (str_date, end_date))

        return df_price

    def get_df_mcpi(self, cursor, str_date='17-01-01', end_date='23-12-31'):
        sql = "SELECT DISTINCT date, price FROM daily_mcpi WHERE (date BETWEEN '%s' AND '%s') ORDER BY date" % ('20'+str_date, '20'+end_date)
        df_mcpi = db(cursor, sql).dataframe

        df_mcpi = df_mcpi.set_index('date')
        df_mcpi.columns = [0]

        df_mcpi = np.transpose(df_mcpi)
        df_mcpi.to_pickle("../storage/df_raw_data/df_mcpi_%s_%s.pkl" % (str_date, end_date))

        return df_mcpi

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
        df_mcpi = pd.read_pickle("../storage/df_raw_data/df_mcpi_17-01-01_23-12-31.pkl")
        df_song_volume = pd.read_pickle("../storage/df_raw_data/df_song_volume_17-01-01_23-12-31.pkl")

        df_mcpi_volume = np.transpose(pd.DataFrame(df_song_volume.sum()))
        df_fng_index = pd.concat([df_mcpi, df_mcpi_volume], axis=0)
        df_fng_index.index = ['mcpi', 'total_volume']

        df_mcpi_volume.to_pickle("../storage/df_raw_data/df_mcpi_volume_%s_%s.pkl" % (str_date, end_date))
        df_fng_index.to_pickle("../storage/df_raw_data/df_fng_index_%s_%s.pkl" % (str_date, end_date))

        return df_mcpi_volume, df_fng_index

