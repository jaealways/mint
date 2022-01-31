import numpy as np

from data_transformation.db_env import DbEnv, db

import pandas as pd
from tqdm import tqdm


class DataTidying:
    """모든 데이터프레임 생성시 column: date, row: num으로 되게 통일하기"""

    def get_list_from_sql(self, cursor, sql):
        list_music_num = DbEnv().get_data_from_table(cursor, sql)
        list_music_num = [item[0] for item in list_music_num]

        return list_music_num

    def get_df_price(self, cursor, str_date='17-01-01', end_date='23-12-31'):
        df_price = pd.DataFrame()
        df_mcpi = pd.read_pickle("../storage/df_raw_data/df_mcpi_17-01-01_23-12-31.pkl")
        list_mcpi_date = df_mcpi.loc[:, '20' + str_date: '20' + end_date].columns.tolist()

        for date in tqdm(list_mcpi_date):
            sql = "SELECT DISTINCT num, price_close FROM daily_music_cow WHERE date='%s' ORDER BY num" % (date)
            df_temp = db(cursor, sql).dataframe
            df_temp = df_temp.set_index('num')
            df_temp.columns = ["%s" % date]

            df_price = pd.concat([df_price, df_temp], axis=1)

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

        copyright_prices = pd.read_pickle("../storage/df_raw_data/copyright_prices.pkl")
        list_ym = ['num']

        for y in range(2017, 2022):
            for m in range(1, 13):
                s_y, s_m = str(y), str(m)
                if len(s_m) == 1:
                    s_m = "0" + s_m
                list_ym.append(s_y + '-' + s_m )

        df_copyright_prices = pd.DataFrame([])

        for key, val in copyright_prices.items():
            list_temp = [key]
            mode = 'zero'
            for idx_y, val_y in enumerate(val):
                for idx_m, val_m in enumerate(val_y):
                    if len(list_temp) == 1:
                        if val_m == '0':
                            mode = 'nan'
                    if val_m != '0':
                        mode = 'zero'
                    if mode == 'nan':
                        val_m = np.nan
                    else:
                        val_m = int(val_m.replace(',', ''))
                    list_temp.append(val_m)

            df_temp = pd.DataFrame(list_temp).T
            df_copyright_prices = pd.concat([df_copyright_prices, df_temp])

        df_copyright_prices.columns = list_ym
        df_copyright_prices = df_copyright_prices.set_index('num')
        df_copyright_prices.to_pickle("../storage/df_raw_data/df_copyright.pkl")

        return df_copyright_prices


conn, cursor = DbEnv().connect_sql()
list_music_num = DataTidying().get_list_song_artist(cursor)