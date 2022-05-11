import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from multiprocessing import Process, Pool
from fng.score import Score, FearGreed

from data_transformation.db_env import DbEnv, db
from data_preprocessing.data_tidying import DataTidying
from data_transformation.mongo_to_sql import MongoToSQL


class SongAnalytics:
    def beta_index(self, df_price, df_mcpi, duration=365):
        list_date, list_song = df_mcpi.columns.tolist(), df_price.index.tolist()
        array_mcpi, array_price = df_mcpi.to_numpy(), df_price.to_numpy()

        array_diff_mcpi, array_diff_price = np.diff(array_mcpi), np.diff(array_price)

        array_return_mcpi = np.true_divide(array_diff_mcpi, array_mcpi[:, :-1])
        array_return_price = np.true_divide(array_diff_price, array_price[:, :-1])

        array_corr = np.corrcoef(array_return_mcpi[0], array_return_price[0])
        array_var = np.var(array_return_price, axis=1)
        array_beta = np.multiply(array_corr[0, 1], array_var / np.var(array_return_mcpi[0]))

        df_beta = pd.DataFrame(array_beta)
        df_beta.columns, df_beta.index = list_date[duration:], list_song
        df_beta.dropna(inplace=True)
        df_beta['rank'] = df_beta.iloc[:, 0].rank(method='max', ascending=False)

        for idx, row in df_beta.iterrows():
            if str(row[list_date[duration:][0]]) == 'nan':
                pass
            else:
                tuple_insert = (list_date[duration:][0], int(idx), float(row[0]), int(row['rank']))
                MongoToSQL().update_daily_beta(tuple_insert)

    def per_duration(self, df_price, df_copyright, duration=12):
        df_price, df_copyright = df_price.sort_index(), df_copyright.sort_index()
        date_13m = (datetime.today() - relativedelta(months=duration)).strftime('%Y-%m')
        df_copyright = df_copyright.loc[:, date_13m:].iloc[:, :duration]
        df_copyright_mean = pd.DataFrame(df_copyright.mean(axis=1, skipna=False), columns=[df_price.columns[-1]]).mul(12)

        df_per = pd.DataFrame(df_price.iloc[:, -1]).div(df_copyright_mean)
        df_per.dropna(inplace=True)
        df_per['rank'] = df_per.iloc[:, 0].rank(method='max', ascending=False)

        for idx, row in df_per.iterrows():
            if str(row.values[-1]) == 'nan':
                pass
            else:
                tuple_insert = (df_per.columns[0], int(idx), float(row[0]), int(row['rank']))
                MongoToSQL().update_daily_per(tuple_insert)

    def market_cap(self, df_price, df_stock):
        df_price, df_stock = df_price.sort_index(), df_stock.sort_index()
        df_stock.columns = [df_price.columns[-1]]
        df_market_cap = pd.DataFrame(df_price.iloc[:, -1]).mul(df_stock)
        df_market_cap.dropna(inplace=True)
        df_market_cap['rank'] = df_market_cap.iloc[:, 0].rank(method='max', ascending=False)

        for idx, row in df_market_cap.iterrows():
            tuple_insert = (df_market_cap.columns[0], int(idx), float(row[0]), int(row['rank']))
            MongoToSQL().update_daily_marketcap(tuple_insert)


    def fng_index(self, df_price, df_price_volume, duration=365):
        x, y = df_price.to_numpy(), df_price_volume.to_numpy()
        score = Score(x, y)
        score_fng = FearGreed(x, y, score).compute(duration=duration)
        df_fng = pd.DataFrame(pd.DataFrame(score_fng, columns=[df_price.columns[duration+1:]]).iloc[:, -1])
        df_fng.index = df_price.index
        df_fng.dropna(inplace=True)
        df_fng['rank'] = df_fng.iloc[:, 0].rank(method='max', ascending=False)

        for idx, row in df_fng.iterrows():
            if str(row.values[0]) == 'nan':
                pass
            else:
                tuple_insert = (df_fng.columns[0][0], int(idx), float(row.values[0]), int(row['rank']))
                MongoToSQL().update_daily_fng(tuple_insert)

    def turn_over(self, df_price_volume, df_stock):
        df_sum = pd.DataFrame(df_price_volume.mean(axis=1, skipna=False)).mul(365)
        df_turnover = df_sum.div(df_stock)
        df_turnover.columns = [df_price_volume.columns[-1]]
        df_turnover.dropna(inplace=True)
        df_turnover['rank'] = df_turnover.iloc[:, 0].rank(method='max', ascending=False)

        for idx, row in df_turnover.iterrows():
            if str(row.values[-1]) == 'nan':
                pass
            else:
                tuple_insert = (df_turnover.columns[0], int(idx), float(row[0]), int(row['rank']))
                MongoToSQL().update_daily_turnover(tuple_insert)


conn, cursor = DbEnv().connect_sql()
client = MongoClient('localhost', 27017)
db1 = client.music_cow
col1 = db1.musicCowData

# duration = 365
# df_mcpi = pd.read_pickle("../storage/df_raw_data/df_mcpi_17-01-01_23-12-31.pkl")
# df_price = pd.read_pickle("../storage/df_raw_data/df_price_17-01-01_23-12-31.pkl")
#

if __name__ == '__main__':
    duration = 365
    list_song_num = col1.find({}).distinct('num')
    date_df = (datetime.today() - timedelta(days=duration + 1)).strftime('%Y-%m-%d')

    pool = Pool(20)

    df_price = pd.DataFrame()
    df_price_temp = pool.map(DataTidying().get_df_price, list_song_num)
    for i in df_price_temp:
        df_price = pd.concat([df_price, i], axis=1)
    df_price = df_price.T

    df_copyright = DataTidying().get_df_copyright()
    SongAnalytics().per_duration(df_price, df_copyright)


