import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from multiprocessing import Process, Pool
from fng.score import scoreIndex, scoreStock, FearGreed

from data_transformation.db_env import DbEnv, db
from data_preprocessing.data_tidying import DataTidying
from data_transformation.mongo_to_sql import MongoToSQL


class SongAnalytics:
    def check_duration(self, table):
        sql = f"SELECT distinct date FROM mu_tech.{table} order by date"
        cursor.execute(sql)
        list_date_sql = [x[0] for x in cursor.fetchall()]

        start_date = datetime.strptime(list_date_sql[0], "%Y-%m-%d")
        date_today = datetime.today()-timedelta(days=1)
        list_date_all = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((date_today-start_date).days+1)]
        list_date_input = list(set(list_date_all) - set(list_date_sql))

        return list_date_input

    def beta_index(self, df_price, df_mcpi, duration=365):
        list_song = df_price.index.tolist()
        list_date = self.check_duration('dailybeta')

        for date in list_date:
            date_duration = (datetime.strptime(date, '%Y-%m-%d') - relativedelta(days=duration)).strftime('%Y-%m-%d')
            array_mcpi, array_price = df_mcpi.loc[:,date_duration:date].to_numpy(), df_price.loc[:,date_duration:date].to_numpy()
            if array_mcpi.shape[1]<366:
                continue

            array_diff_mcpi, array_diff_price = np.diff(array_mcpi), np.diff(array_price)

            array_return_mcpi = np.true_divide(array_diff_mcpi, array_mcpi[:, :-1])
            array_return_price = np.true_divide(array_diff_price, array_price[:, :-1])
            array_beta = np.empty((array_return_price.shape[0],1))
            array_beta[:] = np.nan

            for idx, array_temp in enumerate(array_return_price):
                array_corr = np.corrcoef(array_return_mcpi[0], array_temp)
                array_var = np.var(array_temp)
                array_beta[idx] = np.multiply(array_corr[0, 1], array_var / np.var(array_return_mcpi[0]))

            df_beta = pd.DataFrame(array_beta)
            df_beta.columns, df_beta.index = [date], list_song
            df_beta.dropna(inplace=True)
            df_beta['rank'] = df_beta.iloc[:, 0].rank(method='max', ascending=False)

            for idx, row in df_beta.iterrows():
                if str(row.values[0]) == 'nan':
                    pass
                else:
                    tuple_insert = (df_beta.columns[0], int(idx), float(row.values[0]), int(row['rank']))
                    MongoToSQL().update_daily_beta(tuple_insert)


    def per_duration(self, df_price, df_copyright, duration=12):
        df_price, df_copyright = df_price.sort_index(), df_copyright.sort_index()
        list_date = self.check_duration('dailyper')

        for date in list_date:
            date_13m = (datetime.strptime(date, '%Y-%m-%d') - relativedelta(months=duration)).strftime('%Y-%m')
            df_copyright_temp = df_copyright.loc[:, date_13m:].iloc[:, :duration]
            df_copyright_temp = df_copyright_temp.replace(r'^\s*$', np.nan, regex=True)
            df_copyright_mean = pd.DataFrame(df_copyright_temp.mean(axis=1), columns=[date]).mul(12)

            df_per = pd.DataFrame(df_price.loc[:, date]).div(df_copyright_mean)
            df_per.dropna(inplace=True)
            df_per['rank'] = df_per.iloc[:, 0].rank(method='max', ascending=False)

            for idx, row in df_per.iterrows():
                if str(row.values[0]) == 'nan':
                    pass
                else:
                    tuple_insert = (df_per.columns[0], int(idx), float(row.values[0]), int(row['rank']))
                    MongoToSQL().update_daily_per(tuple_insert)

    def market_cap(self, df_price, df_stock):
        df_price, df_stock = df_price.sort_index(), df_stock.sort_index()
        list_date = self.check_duration('dailymarketcap')

        for date in list_date:
            df_stock.columns = [date]
            df_market_cap = pd.DataFrame(df_price.loc[:,date]).mul(df_stock)
            df_market_cap.dropna(inplace=True)
            df_market_cap['rank'] = df_market_cap.rank(method='max', ascending=False)

            for idx, row in df_market_cap.iterrows():
                if str(row.values[0]) == 'nan':
                    pass
                else:
                    tuple_insert = (df_market_cap.columns[0], int(idx), float(row.values[0]), int(row['rank']))
                    MongoToSQL().update_daily_marketcap(tuple_insert)

    def fng_index(self, df_price, df_price_volume, duration=365):
        list_date = self.check_duration('dailyfng where num=0')

        for date in list_date:
            date_13m = (datetime.strptime(date, '%Y-%m-%d') - relativedelta(days=duration+1)).strftime('%Y-%m-%d')
            x, y = df_price.loc[:, date_13m:date].to_numpy(), df_price_volume.loc[:, date_13m:date].to_numpy()
            if x.shape[1]<duration+1 or y.shape[1]<duration+1:
                continue
            score = scoreIndex(x, y)
            score_fng = FearGreed(score).compute_index(duration=duration)
            df_fng = pd.DataFrame(pd.DataFrame(score_fng, columns=[date]).iloc[:, -1])
            df_fng.index = df_price.index
            df_fng.dropna(inplace=True)
            df_fng['rank'] = df_fng.iloc[:, 0].rank(method='max', ascending=False)

            for idx, row in df_fng.iterrows():
                if str(row.values[0]) == 'nan':
                    pass
                else:
                    tuple_insert = (df_fng.columns[0], int(idx), float(row.values[0]), int(row['rank']))
                    MongoToSQL().update_daily_fng(tuple_insert)

    def fng_stock(self, df_price_end, df_price_high, df_price_low, df_price_volume, duration=120):
        list_date = self.check_duration('dailyfng where num>0')

        for date in list_date:
            date_13m = (datetime.strptime(date, '%Y-%m-%d') - relativedelta(days=duration+1)).strftime('%Y-%m-%d')
            a, b, c, y = df_price_end.loc[:, date_13m:date].to_numpy(), df_price_high.loc[:, date_13m:date].to_numpy(), \
                         df_price_low.loc[:, date_13m:date].to_numpy(), df_price_volume.loc[:, date_13m:date].to_numpy()
            if a.shape[1]<duration+1 or b.shape[1]<duration+1 or c.shape[1]<duration+1 or y.shape[1]<duration+1:
                continue
            score = scoreStock(a, b, c, y)
            score_fng = FearGreed(score).compute_stock(duration=duration)
            df_fng = pd.DataFrame(pd.DataFrame(score_fng, columns=[date]).iloc[:, -1])
            df_fng.index = df_price_end.index
            df_fng.dropna(inplace=True)
            df_fng['rank'] = df_fng.iloc[:, 0].rank(method='max', ascending=False)

            for idx, row in df_fng.iterrows():
                if str(row.values[0]) == 'nan':
                    pass
                else:
                    tuple_insert = (df_fng.columns[0], int(idx), float(row.values[0]), int(row['rank']))
                    MongoToSQL().update_daily_fng(tuple_insert)

    def fng_index_plot(self, df_price, df_price_high, df_price_low, df_price_volume, duration=365):
        a,b,c,y = df_price.to_numpy(), df_price_high.to_numpy(), df_price_low.to_numpy(), df_price_volume.to_numpy()
        score_index = scoreIndex(a,y)
        score_stock = scoreStock(a,b,c,y)
        score_fng_index = FearGreed(score_index).compute_index(duration=duration)
        score_fng_stock = FearGreed(score_stock).compute_stock(duration=duration)
        df_fng_index = pd.DataFrame(score_fng_index)
        df_fng_stock = pd.DataFrame(score_fng_stock)

        df_fng_index.index, df_fng_stock.index = df_price.index, df_price.index
        # df_fng.dropna(inplace=True)

        return df_fng_index, df_fng_stock

    def turn_over(self, df_price_volume, df_stock):
        list_date = self.check_duration('dailyturnover')

        for date in list_date:
            date_13m = (datetime.strptime(date, '%Y-%m-%d') - relativedelta(days=365)).strftime('%Y-%m-%d')
            df_sum = pd.DataFrame(df_price_volume.loc[:, date_13m:date].mean(axis=1, skipna=False)).mul(365)
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
# df_price = pd.read_pickle("../storage/df_raw_data/df_price.pkl")
# df_price_volume = pd.read_pickle("../storage/df_raw_data/df_volume.pkl")
#

if __name__ == '__main__':
    duration=720
    list_song_num = col1.find({}).distinct('num')
    date_today = datetime.strptime('2022-08-01', '%Y-%m-%d')
    date_df = (datetime.today() - timedelta(days=duration+2)).strftime('%Y-%m-%d')
    pool = Pool(20)

    df_price = pd.read_pickle("../storage/df_raw_data/df_price.pkl")
    df_mcpi = DataTidying().get_df_mcpi('2022-10-31')
    df_mcpi = pd.DataFrame(df_mcpi.iloc[:,:-df_price.shape[1]])
    # df_price.to_pickle('../storage/df_raw_data/df_price.pkl')

    SongAnalytics().beta_index(df_price, df_mcpi)

    # mcpi 하기
    # df_mcpi
    # SongAnalytics().fng_stock_temp(df_price, df_price_high, df_price_low, df_price_volume, duration=120)

    # df_price_volume = pd.read_pickle("../storage/df_raw_data/df_volume.pkl")
    # df_price_high = pd.read_pickle("../storage/df_raw_data/df_price_high.pkl")
    # df_price_low = pd.read_pickle("../storage/df_raw_data/df_price_low.pkl")
    # df_fng_index, df_fng_stock = SongAnalytics().fng_stock(df_price, df_price_high, df_price_low, df_price_volume, duration=120)
    #
    # import matplotlib.pyplot as plt
    # index_list = df_price_volume.index.tolist()
    #
    # num_mc = 903
    # num = index_list.index(num_mc)
    #
    # fig, axs = plt.subplots(4)
    # axs[0].plot(df_price.to_numpy()[num, -df_fng_index.shape[1]:])
    # axs[1].plot(df_price_volume.to_numpy()[num, -df_fng_index.shape[1]:])
    # axs[2].plot(df_fng_index.to_numpy()[num, :])
    # axs[3].plot(df_fng_stock.to_numpy()[num, :])
    #
    # plt.show()

    # for date in list_date:
    #     print(date)
    #     date_df = (datetime.today() - timedelta(days=duration + 2)).strftime('%Y-%m-%d')
    #     df_price = df_price.loc[date_df:,:date]
    #     df_price_volume = df_price_volume.iloc[date_df:,:date]
    #
    #     SongAnalytics().fng_index(df_price, df_price_volume, duration=120)

