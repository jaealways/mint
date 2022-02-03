import pandas as pd
import numpy as np
import sys

import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler


df_beta = pd.read_pickle("../storage/df_raw_data/df_beta.pkl")
df_per_12 = pd.read_pickle("../storage/df_raw_data/df_per_month_12.pkl")
df_list = pd.read_pickle("../storage/df_raw_data/df_list_song_artist.pkl")

list_date = sorted(list(set.intersection(set(df_per_12.columns.tolist()), set(df_beta.columns.tolist()))))
df_beta_temp, df_per_temp = df_beta.loc[:, list_date], df_per_12.loc[:, list_date]


def not_outliers_iqr(data):
    q1, q3 = np.percentile(data, [25, 75])
    iqr = q3 - q1
    ub, lb = q1 + (1.5 * iqr), q1 - (1.5 * iqr)

    return np.where((data < ub) & (data >= lb))


def onclick(event):
    df_temp['beta'] = (df_plot.T['beta'] - event.xdata).abs()
    df_temp['per'] = (df_plot.T['per'] - event.ydata).abs()

    min_max_scaler = MinMaxScaler()
    fitted = min_max_scaler.fit(df_temp)
    output = min_max_scaler.transform(df_temp)
    df_output = pd.DataFrame(output, columns=df_temp.columns, index=list(df_temp.index.values))
    song_num = df_plot.columns.tolist()[df_output.sum(axis=1).argsort().iloc[0]]

    temp1 = df_list.loc[song_num].tolist()
    title, artist = temp1[0], temp1[1]
    temp2 = df_plot.loc[:, song_num].tolist()
    p_beta, p_per = temp2[0], temp2[1]
    print('song_num=%s, artist=%s, title=%s beta=%f, per=%f' % (song_num, artist, title, p_beta, p_per))
    # event.xdata, event.ydata

def on_press(event):
    print('press', event.key)
    sys.stdout.flush()
    if event.key == 'right':
        mode = 'pass'


for date in list_date:
    date = '2021-09-08'
    mode = 'stay'
    df_beta_temp_d, df_per_temp_d = df_beta_temp.loc[:, date].dropna(axis=0), df_per_temp.loc[:, date].dropna(axis=0)
    idx_beta, idx_per = not_outliers_iqr(df_beta_temp_d), not_outliers_iqr(df_per_temp_d)
    df_beta_d = pd.DataFrame(df_beta_temp_d.iloc[idx_beta])
    df_per_d = pd.DataFrame(df_per_temp_d.iloc[idx_per])

    df_plot = pd.concat([df_beta_d.T, df_per_d.T], join='inner')
    df_plot.index = ['beta', 'per']
    df_plot = df_plot.dropna(axis=1)


    # from sklearn.cluster import KMeans
    #
    # n_clusters = 3
    # kmeans = KMeans(n_clusters=n_clusters, init='k-means++', max_iter=3000, n_init=10)
    # y_pred = kmeans.fit_predict(df_plot.T)
    # df_plot = df_plot.append(pd.DataFrame(y_pred, index=df_plot.columns).T)
    # df_plot.index = ['beta', 'per', 'label']

    # 실루엣 계수로 클러스터 갯수 정하기 -> 3개
    # from sklearn.metrics import silhouette_samples, silhouette_score
    #
    # range_n_clusters = [2, 3, 4, 5, 6]
    # for n_clusters in range_n_clusters:
    #     clusterer = KMeans(n_clusters=n_clusters, random_state=10)
    #     cluster_labels = clusterer.fit_predict(df_plot.T)
    #     silhouette_avg = silhouette_score(df_plot.T, cluster_labels)
    #     print(silhouette_avg)

    df_temp = df_plot.T.copy()

    fig, ax = plt.subplots(1)
    plt.xlim(-1, 2)
    plt.ylim(0, 80)

    ax.scatter(df_plot.T['beta'], df_plot.T['per'])
    # ax.scatter(df_plot.T['beta'], df_plot.T['per'], c=df_plot.T['label'].astype(float))


    plt.title(date)
    plt.xlabel('BETA')
    plt.ylabel('PER')
    fig.canvas.callbacks.connect('button_press_event', onclick)
    fig.canvas.mpl_connect('key_press_event', on_press)
    if mode == 'pass':
        continue
    plt.show()

