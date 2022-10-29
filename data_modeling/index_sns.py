import sys
sys.path.append("..")

import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from multiprocessing import Process, Pool
import matplotlib.pyplot as plt
from tqdm import tqdm

from data_transformation.db_env import DbEnv, db
from data_preprocessing.data_tidying import DataTidying


conn, cursor = DbEnv().connect_sql()


def get_youtube_index(num):
    sql = 'SELECT distinct video_num from mu_tech.daily_youtube where song_num=%s' % (num)
    cursor.execute(sql)
    conn.commit()
    list_temp = cursor.fetchall()
    list_num = [x[0] for x in list_temp]

    pool = Pool(30)

    df_youtube = pd.DataFrame()
    df_price_temp = pool.map(get_df_youtube, list_num)
    for i in df_price_temp:
        df_youtube = pd.concat([df_youtube, i], axis=1)
    df_youtube = df_youtube.T

    return df_youtube


def get_df_youtube(num):
    sql = 'SELECT date, viewCount from mu_tech.daily_youtube where video_num=%s ORDER by date' % (num)
    df_temp = db(cursor, sql).dataframe
    df_temp = df_temp.set_index('date')
    df_temp.columns = [num]

    return df_temp

def get_df_price(num):
    sql = 'SELECT price_close, date from mu_tech.musiccowdata where num=%s ORDER by date' % (num)
    df_temp = db(cursor, sql).dataframe
    df_temp = df_temp.set_index('date')
    df_temp.columns = [num]

    return df_temp


def get_average_index(df_youtube):
    df_sa = df_youtube.mean()
    df_lwa_temp = 1 + df_youtube.diff(axis=1).iloc[:, 1:].div(df_youtube.iloc[:, 1:]).mean()
    df_lwa_temp[df_youtube.columns[0]] = 1
    df_lwa_temp = df_lwa_temp.sort_index()
    df_lwa_first = df_youtube.iloc[:, 0].mean(axis=0)
    df_lwa = pd.Series()
    df_lwa[df_youtube.columns[0]] = df_lwa_first

    for idx, row in enumerate(df_lwa_temp[1:].items()):
        df_lwa[row[0]] = row[1] * df_lwa[idx]

    df_sa, df_lwa = df_sa.to_frame(), df_lwa.to_frame()

    return df_sa, df_lwa


def plot_price(num):
    df_youtube = get_youtube_index(num)
    df_sa, df_lwa = get_average_index(df_youtube)
    df_price = get_df_price(num)
    df_price = df_price.loc[df_sa.index.tolist(), :]

    df_plot = pd.concat([df_price, df_sa], axis=1)
    df_plot = pd.concat([df_plot, df_lwa], axis=1)

    df_plot.columns = ['price', 'sa', 'lwa']

    df_plot.plot()

    plt.show()

def detect_ratio():
    sql = 'SELECT distinct song_num from mu_tech.daily_youtube'
    cursor.execute(sql)
    conn.commit()
    list_temp = cursor.fetchall()
    list_num = [x[0] for x in list_temp]

    for num in tqdm(list_num[70:]):
        try:
            get_youtube_threshold(num)
        except:
            pass


def get_youtube_threshold(num):
    threshold = 0.05

    df_youtube = get_youtube_index(num)
    df_sa, df_lwa = get_average_index(df_youtube)

    df_sa_temp = df_sa.T.diff(axis=1).iloc[:, 1:].div(df_sa.T.iloc[:, 1:]) - 1
    df_lwa_temp = df_lwa.T.diff(axis=1).iloc[:, 1:].div(df_lwa.T.iloc[:, 1:]) - 1

    temp1, temp2 = df_sa_temp.ge(threshold), df_lwa_temp.ge(threshold)
    list_sa, list_lwa = np.where(temp1)[0].tolist(), np.where(temp2)[0].tolist()

    if len(list_sa) + len(list_lwa) > 0:
        print('출력')
        print('sa: ', list_sa)
        print('lwa: ', list_lwa)

def btw_per_beta():
    date = '2021-05-24'
    sql = "SELECT num, beta from mu_tech.dailybeta where date='%s'" % (date)
    df_beta = db(cursor, sql).dataframe
    df_beta = df_beta.set_index('num')
    sql = "SELECT num, per from mu_tech.dailyper where date='%s'" % (date)
    df_per = db(cursor, sql).dataframe
    df_per = df_per.set_index('num')
    df_plot = pd.concat([df_beta, df_per], axis=1, join='inner')
    df_plot.plot.scatter(x='beta', y='per')
    plt.show()

def btw_mcpi():
    sql = 'SELECT distinct date from mu_tech.dailyturnover'
    cursor.execute(sql)
    conn.commit()
    list_temp = cursor.fetchall()
    list_num = [x[0] for x in list_temp]

    pool = Pool(30)

    df_youtube = pd.DataFrame()
    df_price_temp = pool.map(get_df_for_mcpi, list_num)
    for i in tqdm(df_price_temp):
        df_youtube = pd.concat([df_youtube, i], axis=1)
    df_youtube = df_youtube.T
    df_youtube = pd.DataFrame(df_youtube.mean(axis=1))
    # df_youtube = df_youtube[df_youtube.le(50)]

    sql = "SELECT date, price from mu_tech.dailymcpi"
    df_mcpi = db(cursor, sql).dataframe
    df_mcpi = df_mcpi.set_index('date')
    df_mcpi = df_mcpi.sort_index()
    df_mcpi = df_mcpi.diff().iloc[1:, :] / df_mcpi.iloc[1:, :]
    df_plot = pd.concat([df_youtube, df_mcpi], axis=1, join='inner')
    df_plot.columns = ['tno', 'mcpi']
    df_plot.plot.scatter(x='tno', y='mcpi')
    plt.show()


def get_df_for_mcpi(date):
    sql = "SELECT num, tno from mu_tech.dailyturnover where date='%s'" % date
    df_temp = db(cursor, sql).dataframe
    df_temp = df_temp.set_index('num')
    df_temp.columns = [date]

    return df_temp

if __name__ == '__main__':
    btw_mcpi()

