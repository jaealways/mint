import numpy as np

class FearandGreedIndex:
    def __init__(self):
        print('s')


import sys
sys.path.append("..")

import math
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from data_transformation.db_env import DbEnv, db
from data_preprocessing.data_tidying import DataTidying
from data_preprocessing.data_preprocess import DataPreprocess


# 노래 번호 리스트 출력
conn, cursor = DbEnv().connect_sql()
sql = "SELECT DISTINCT num FROM daily_music_cow ORDER BY num"
list_music_num = DataTidying().get_list_from_sql(cursor, sql)

# 데이터 프레임 생성
# df_song_volume = DataTidying().get_df_song_volume(cursor)
# df_mcpi_volume, df_fng_index = DataTidying().get_df_fng_index()

# 가장 최근에 저장된 값 불러올 경우 피클 이용

df_mcpi_volume = pd.read_pickle("../storage/df_raw_data/df_mcpi_volume_17-01-01_23-12-31.pkl")
df_fng_index = pd.read_pickle("../storage/df_raw_data/df_fng_index_17-01-01_23-12-31.pkl")

# 거래량, mcpi 지수 표시 plt

df_mcpi_volume_t, df_fng_index_t = df_mcpi_volume.T, df_fng_index.T
df_mcpi_yesterday_t = df_fng_index_t['mcpi'][:-1]
df_mcpi_yesterday_t = pd.concat([pd.DataFrame([0]), df_mcpi_yesterday_t], ignore_index=True)
df_mcpi_yesterday_t.index = df_fng_index_t.index
df_mcpi_yes_log_t, df_mcpi_log_t = pd.DataFrame(np.log(df_mcpi_yesterday_t))[1:], pd.DataFrame(np.log(df_fng_index_t['mcpi']))[1:]

# column 같게 만들기
df_mcpi_yes_log_t.columns = df_mcpi_log_t.columns

df_mcpi_return_t = df_mcpi_log_t.subtract(df_mcpi_yes_log_t, axis=0)
df_mcpi_return_ewm_t = df_mcpi_return_t.ewm(alpha=0.94).mean()
df_mcpi_return_ewm_t = pd.concat([pd.DataFrame([0], columns=['mcpi']), df_mcpi_return_ewm_t], ignore_index=True)

df_mcpi_return_ewm_t.index = df_fng_index_t.index


print('# Eq 1-1')

iter, lam = 0, 0.94
df_mcpi_return_sigma_2_t = pd.DataFrame([0], columns=['score_volatility'])

for x in df_fng_index.T['mcpi']:
    if iter == 0:
        sigma_2 = 0
    iter += 1
    sigma_2_new = lam * sigma_2 + (1 - lam)*(x**2)
    df_mcpi_return_sigma_2_t = pd.concat([df_mcpi_return_sigma_2_t, pd.DataFrame([sigma_2_new], columns=['score_volatility'])], axis=0)
    sigma_2 = sigma_2_new

df_mcpi_return_sigma_2_t = df_mcpi_return_sigma_2_t[:-1]
df_mcpi_return_sigma_2_t.index = df_fng_index_t.index


print('# Eq 1-2')
df_mcpi_return_sigma_t = df_mcpi_return_sigma_2_t.apply(lambda x: np.sqrt(x))
df_mcpi_return_sigma_mu_t, df_mcpi_return_sigma_sigma_t = pd.DataFrame(), pd.DataFrame()

for len_date in range(len(df_mcpi_return_sigma_t.index)-365):
    df_date = df_mcpi_return_sigma_t.iloc[len_date:len_date+365]
    df_date_log = df_date.apply(lambda x: np.log(x))

    df_date_log = df_date_log.drop(df_date_log[df_date_log['score_volatility']==-np.inf].index)
    date_log_mu, date_log_std = df_date_log.mean(axis=0).values, df_date_log.std(axis=0).values
    df_mcpi_return_sigma_mu_t = pd.concat([df_mcpi_return_sigma_mu_t, pd.DataFrame([date_log_mu], index=[df_date.index[-1]])], axis=0)
    df_mcpi_return_sigma_sigma_t = pd.concat([df_mcpi_return_sigma_sigma_t, pd.DataFrame([date_log_std], index=[df_date.index[-1]])], axis=0)

df_score_volatility_t = pd.DataFrame()

for date in df_mcpi_return_sigma_mu_t.index:
    test_1 = np.log(df_mcpi_return_sigma_t.loc[date]).values
    test_2 = df_mcpi_return_sigma_mu_t.loc[date].values
    test_3 = df_mcpi_return_sigma_sigma_t.loc[date].values
    score_temp = (test_1 - test_2) / test_3
    score = min(max(-4, score_temp), 4)
    df_score_volatility_t = pd.concat([df_score_volatility_t, pd.DataFrame([score], index=[date])], axis=0)


print('# Eq 1-3')

n_long, n_short = 60, 20

def get_volume_ewm(df, n):
    df_mcpi_volume_ewm_t = pd.DataFrame()
    lambda_n = 1 - 1/n
    for idx, val in enumerate(df.values):
        mcpi_volume_ewm = 0
        for num in range(idx+1):
            mcpi_volume_ewm += (1-lambda_n)*(0.94**num)*val
        df_mcpi_volume_ewm_t = pd.concat([df_mcpi_volume_ewm_t, pd.DataFrame([mcpi_volume_ewm])], axis=0)
    return df_mcpi_volume_ewm_t


df_mcpi_volume_ewm_long_t, df_mcpi_volume_ewm_short_t = get_volume_ewm(df_mcpi_volume_t, n_long), get_volume_ewm(df_mcpi_volume_t, n_short)
df_mcpi_volume_ewm_long_t.index, df_mcpi_volume_ewm_short_t.index = df_mcpi_volume_t.index, df_mcpi_volume_t.index

df_mcpi_volume_ewm_sqrt_t = pd.DataFrame()
df_temp_long_t = pd.concat([df_mcpi_volume_t, df_mcpi_volume_ewm_long_t], axis=1)
df_temp_short_t = pd.concat([df_mcpi_volume_t, df_mcpi_volume_ewm_short_t], axis=1)
df_temp_long_t.columns, df_temp_short_t.columns = [0, 1], [0, 1]

# df_mcpi_volume_ewm_sqrt_long_t = np.log(df_mcpi_volume_t.div(df_mcpi_volume_ewm_long_t))
# df_mcpi_volume_ewm_sqrt_short_t = np.log(df_mcpi_volume_t.div(df_mcpi_volume_ewm_short_t))


# def get_mcpi_volume_ewm_sqrt(df_1, df_2):
#     df_mcpi_volume_ewm_sqrt_t = pd.DataFrame()
#     for date in df_1.index:
#         aws, aws_1, aws_2 = 0, df_1.loc[date], df_2.loc[date]
#         if aws_2.values[0] == 0:
#             pass
#         else:
#             aws = np.log(aws_1/aws_2)
#         print(aws_1.values[0], aws_2.values[0], aws)
#         df_mcpi_volume_ewm_sqrt_t = pd.concat([df_mcpi_volume_ewm_sqrt_t, pd.DataFrame([aws])], axis=0)
#     return df_mcpi_volume_ewm_sqrt_t

df_mcpi_volume_ewm_sqrt_long_t = np.transpose(df_temp_long_t.T.apply(lambda x: np.log(x[0]/x[1]))).T
df_mcpi_volume_ewm_sqrt_short_t = np.transpose(df_temp_short_t.T.apply(lambda x: np.log(x[0]/x[1]))).T

# df_mcpi_volume_ewm_sqrt_long_t = get_mcpi_volume_ewm_sqrt(df_mcpi_volume_t, df_mcpi_volume_ewm_long_t)
# df_mcpi_volume_ewm_sqrt_short_t = get_mcpi_volume_ewm_sqrt(df_mcpi_volume_t, df_mcpi_volume_ewm_short_t)


df_mcpi_volume_ewm_sqrt_long_t.index, df_mcpi_volume_ewm_sqrt_short_t.index = df_mcpi_volume_t.index, df_mcpi_volume_t.index
df_score_volume_t = pd.DataFrame()

for date in df_mcpi_volume_ewm_sqrt_long_t.index:
    test_1 = df_mcpi_volume_ewm_sqrt_long_t.loc[date]
    test_2 = df_mcpi_volume_ewm_sqrt_short_t.loc[date]
    score = min(max(-4, (test_1 + test_2)/2), 4)
    df_score_volume_t = pd.concat([df_score_volume_t, pd.DataFrame([score], index=[date])], axis=0)


print('# Eq 1-5')

df_score_vv_t = pd.DataFrame()

list_date_vv = sorted(list(set(df_score_volume_t.index).intersection(df_score_volatility_t.index)))

for date in list_date_vv:
    test_1 = df_score_volatility_t.loc[date].values[-1]
    test_2 = df_score_volume_t.loc[date].values[-1]
    score = (min(max(-4, test_1 + test_2), 4))/8 + 0.5
    df_score_vv_t = pd.concat([df_score_vv_t, pd.DataFrame([score], index=[date])], axis=0)

# plt_temp = df_fng_index_temp.plot(x='index', y='mcpi', c='b')
# df_fng_index_temp.plot(ax=plt_temp, x='index', y='mcpi_ewm', c='r')
# plt.show()


print('# Eq 2-1')

df_l_short_t = df_score_vv_t.apply(lambda x: (9*x)+1)
df_l_long_t = df_l_short_t.apply(lambda x: 10-x)


print('# Eq 2-2')

def get_df_mcpi_ewm(lam, df):
    df_mcpi_mean = pd.DataFrame()
    for idx, val in enumerate(df):
        mcpi_mean = 0
        for num in range(idx+1):
            mcpi_mean += (1-lam)*(lam**(num-idx))*val
        df_mcpi_mean = pd.concat([df_mcpi_mean, pd.DataFrame([mcpi_mean])], axis=0)
    return df_mcpi_mean


lambda_long, lambda_short = 1 - 1/30, 1 - 1/7
df_mcpi_long_ewm_t, df_mcpi_short_ewm_t = get_df_mcpi_ewm(lambda_long, df_fng_index_t['mcpi']), get_df_mcpi_ewm(lambda_short, df_fng_index_t['mcpi'])
df_mcpi_long_ewm_t.index, df_mcpi_short_ewm_t.index = df_fng_index_t['mcpi'].index, df_fng_index_t['mcpi'].index

df_x_temp_long_t = pd.concat([df_fng_index_t['mcpi'], df_mcpi_long_ewm_t], axis=1)
df_x_temp_short_t = pd.concat([df_fng_index_t['mcpi'], df_mcpi_short_ewm_t], axis=1)
df_x_temp_long_t.columns, df_x_temp_short_t.columns = [0, 1], [0, 1]

df_mcpi_x_long = df_x_temp_long_t.T.apply(lambda x: (x[0]-x[1])/x[1]).T
df_mcpi_x_short = df_x_temp_short_t.T.apply(lambda x: (x[0]-x[1])/x[1]).T


print('# Eq 2-3')

c = 30
df_score_moment_temp_t = pd.concat([df_l_long_t, df_mcpi_x_long, df_l_short_t, df_mcpi_x_short], axis=1)
df_score_moment_temp_t.columns = [0, 1, 2, 3]
df_score_momentum = df_score_moment_temp_t.T.apply(lambda x: c*(x[0]*x[1]+x[2]*x[3])/10)
df_score_momentum = df_score_momentum.dropna()


print('# Eq 3')

lambda_7, lambda_2 = 1 - 1/7, 1 - 1/2

def get_df_w_ewm(df, lam):
    df_mcpi_w = pd.DataFrame()
    list_date = df_score_momentum.index
    for idx, val in enumerate(list_date):
        mcpi_w = 0
        for num in range(idx+1):
            mcpi_w += (1-lam)*(lam**(num-idx))*df.loc[val]
        df_mcpi_w = pd.concat([df_mcpi_w, pd.DataFrame([mcpi_w])], axis=0)

    return df_mcpi_w


df_mcpi_w_ewm_short_t, df_mcpi_w_ewm_long_t = get_df_w_ewm(df_score_momentum, lambda_2), get_df_w_ewm(df_score_momentum, lambda_7)

df_mcpi_w_ewm_t = pd.DataFrame(df_mcpi_w_ewm_short_t.add(df_mcpi_w_ewm_long_t).div(2))

df_mcpi_beta_t = df_mcpi_w_ewm_t.T.apply(lambda x: 2+abs(x)-(4/(1+math.exp(abs(-x)))))

list_idx_w = df_mcpi_w_ewm_t

df_fng_index_t['mcpi']

# plt_temp = df_fng_index_t['mcpi'].plot(x='index', y='mcpi', c='b')
# df_fng_index_temp.plot(ax=plt_temp, x='index', y='mcpi_ewm', c='r')
# plt.show()


