import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from data_transformation.db_env import DbEnv
from data_preprocessing.data_tidying import DataTidying
from data_transformation.mongo_to_sql import MongoToSQL


class SongAnalytics:
    def beta_index(self, df_price, df_mcpi, duration=365):
        # date = '2021-04-28'

        list_date, list_song = df_mcpi.columns.tolist(), df_price.index.tolist()
        array_mcpi, array_price = df_mcpi.to_numpy(), df_price.to_numpy()

        array_diff_mcpi, array_diff_price = np.diff(array_mcpi), np.diff(array_price)
        array_corr, array_var, array_beta = array_price[:, duration:].copy(), array_price[:, duration:].copy(), array_price[:, duration:].copy()

        array_return_mcpi = np.true_divide(array_diff_mcpi, array_mcpi[:, 1:])
        array_return_price = np.true_divide(array_diff_price, array_price[:, 1:])

        for idx in range(array_return_price.shape[1] - duration + 1):
            array_corr[:, idx] = np.corrcoef(array_return_mcpi[:, idx:idx + duration][0],
                                             array_return_price[:, idx:idx + duration])[0, 1:]
            array_var[:, idx] = np.var(array_return_price[:, idx:idx + duration], axis=1)
            array_beta[:, idx] = np.multiply(array_corr[:, idx],
                                             array_var[:, idx] / np.var(array_return_mcpi[0, idx:idx + duration]))

        df_beta = pd.DataFrame(array_beta)
        df_beta.columns, df_beta.index = list_date[duration:], list_song

        for idx, row in df_beta.iterrows():
            if str(row[list_date[duration:][0]]) == 'nan':
                pass
            else:
                tuple_insert = (list_date[duration:][0], int(idx), float(row[list_date[duration:][0]]))
                MongoToSQL().update_daily_beta(tuple_insert)


    def per_duration(self, date, df_price, df_copyright, duration=365):
        # date = (datetime.today() - relativedelta(months=1)).strftime('%Y-%m')
        list_month, list_song = df_copyright.columns, df_copyright.index

        array_copyright = df_copyright.to_numpy()
        copyright_mean = np.zeros((array_copyright.shape[0], array_copyright.shape[1] - duration + 1))

        for idx, val in enumerate(array_copyright.T[duration - 1:, :]):
            copyright_mean[:, idx] = np.nanmean(array_copyright[:, idx: idx + duration], axis=1)

        df_copyright_mean = pd.DataFrame(copyright_mean)
        df_copyright_mean.columns, df_copyright_mean.index = list_month[duration - 1:], list_song

        df_per = df_price.copy()

        for idx_c, val_c in enumerate(df_price.columns):
            date_copyright = datetime.strptime(val_c, "%Y-%m-%d") - relativedelta(months=1)
            date_copyright = date_copyright.strftime("%Y-%m")
            for idx_i, val_i in enumerate(df_price.index):
                try:
                    val_copy = df_copyright_mean.loc[val_i, date_copyright]
                    df_per.loc[val_i, val_c] = df_price.loc[val_i, val_c] / (val_copy * 12)
                except:
                    df_per.loc[val_i, val_c] = np.nan
                    # 510, 617, 639, 717 등 저작권 없음

        return df_per

    def market_cap(self):
        print('market_cap')


    def fng_index(self):
        print('market_cap')


    def turn_over(self):
        print('market_cap')


conn, cursor = DbEnv().connect_sql()
client = MongoClient('localhost', 27017)
db1 = client.music_cow
col1 = db1.musicCowData

# duration = 365
# df_mcpi = pd.read_pickle("../storage/df_raw_data/df_mcpi_17-01-01_23-12-31.pkl")
# df_price = pd.read_pickle("../storage/df_raw_data/df_price_17-01-01_23-12-31.pkl")
#

if __name__ == '__main__':
    SongAnalytics().per_duration('2022-05-06')

