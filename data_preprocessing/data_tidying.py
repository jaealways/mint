from data_transformation.db_env import DbEnv, db

import pandas as pd
from tqdm import tqdm


class DataTidying:
    def get_list_music_num(self, cursor):
        sql = "SELECT DISTINCT num FROM daily_music_cow"
        list_music_num = DbEnv().get_data_from_table(cursor, sql)
        list_music_num = [item[0] for item in list_music_num]

        return list_music_num

    def get_df_price(self, list_num, cursor, str_date='2017-01-01', end_date='2023-12-31'):
        df_price = pd.DataFrame()
        for num in tqdm(list_num):
            sql = "SELECT date, price FROM daily_music_cow WHERE (num = '%s') AND (date BETWEEN '%s' AND '%s')" % (num, str_date, end_date)
            df_temp = db(cursor, sql).dataframe
            df_temp = df_temp.set_index('date')
            df_temp.columns = ["%d" % num]

            df_price = pd.concat([df_price, df_temp], axis=1)

        df_price.to_pickle("../storage/df_price.pkl")

        return df_price

    def get_df_mcpi(self, cursor):
        sql = "SELECT date, price FROM daily_mcpi"
        df_mcpi = db(cursor, sql).dataframe
        df_mcpi = df_mcpi.set_index('date')
        df_mcpi.columns = [0]

        df_mcpi.to_pickle("../storage/df_mcpi.pkl")

        return df_mcpi


