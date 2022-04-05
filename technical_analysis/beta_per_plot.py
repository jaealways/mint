import pandas as pd
import numpy as np
import sys
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
import seaborn as sns
import time
from gensim.models.word2vec import Word2Vec

from data_modeling.nlp_modeling import NLPClustering


class BetaPlot:
    def not_outliers_iqr(self, data):
        q1, q3 = np.percentile(data, [25, 75])
        iqr = q3 - q1
        ub, lb = q1 + (1.5 * iqr), q1 - (1.5 * iqr)

        return np.where((data < ub) & (data >= lb))

    def onclick(self, event):
        df_temp, df_plot = self.df_temp, self.df_plot
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
        p_beta, p_per, genre = temp2[0], temp2[1], temp2[2]
        print('song_num=%s, artist=%s, title=%s, genre=%s, beta=%f, per=%f' % (song_num, artist, title, genre, p_beta, p_per))
        # event.xdata, event.ydata

    def color_genre(self):
        color_labels = df_genre['genre'].unique()
        rgb_values = sns.color_palette("Set2", len(color_labels))
        color_map = dict(zip(color_labels, rgb_values))

        df_beta_temp, df_per_temp = df_beta.loc[:, list_date], df_per_12.loc[:, list_date]

        return df_beta_temp, df_per_temp, color_map

    def color_artist(self):
        set_artist = set(df_list['artist'].values.tolist())
        model_res = NLPClustering().restrict_w2v(model.wv, set_artist)

        df_artist_cluster = NLPClustering().cluster_artist(model_res)
        df_clu = df_list.copy()
        df_clu['cluster'] = 0

        for x in df_artist_cluster[0].unique():
            list_artist = df_artist_cluster[df_artist_cluster[0] == x][1].values.tolist()
            for y in list_artist:
                update_idx = df_clu[df_clu['artist'] == y].index.tolist()
                df_clu['cluster'][update_idx] = x

        color_labels = df_clu['cluster'].unique()
        rgb_values = sns.color_palette("Set2", len(color_labels))
        color_map = dict(zip(color_labels, rgb_values))

        df_beta_temp, df_per_temp = df_beta.loc[:, list_date], df_per_12.loc[:, list_date]

        return df_beta_temp, df_per_temp, color_map, df_clu

    def plot_genre(self, date, df_beta_temp, df_per_temp, color_map):
        # date = '2021-09-08'
        fig, ax = plt.subplots(1)
        plt.xlim(-2, 7)
        plt.ylim(0, 80)

        df_beta_temp_d, df_per_temp_d = df_beta_temp.loc[:, date].dropna(axis=0), df_per_temp.loc[:, date].dropna(axis=0)
        idx_beta, idx_per = self.not_outliers_iqr(df_beta_temp_d), self.not_outliers_iqr(df_per_temp_d)
        df_beta_d = pd.DataFrame(df_beta_temp_d.iloc[idx_beta])
        df_per_d = pd.DataFrame(df_per_temp_d.iloc[idx_per])

        df_plot = pd.concat([df_beta_d.T, df_per_d.T], join='inner')
        df_plot.index = ['beta', 'per']
        df_plot = df_plot.T.join(df_genre).T
        df_plot = df_plot.dropna(axis=1)

        df_temp = df_plot.T.iloc[:, 0:2].copy()
        self.df_temp, self.df_plot = df_temp, df_plot

        ax.scatter(df_plot.T['beta'], df_plot.T['per'], c=df_plot.T['genre'].map(color_map))

        plt.title(date)
        plt.xlabel('BETA')
        plt.ylabel('PER')

        fig.canvas.callbacks.connect('button_press_event', self.onclick)

        plt.show(block=False)
        plt.pause(0.3)
        plt.close()

        # plt.draw()
        # plt.pause(0.5)
        # fig.clear()

    def plot_artist(self, date, df_beta_temp, df_per_temp, color_map, df_clu):
        # date = '2021-09-08'
        fig, ax = plt.subplots(1)
        plt.xlim(-2, 7)
        plt.ylim(0, 80)
        # plt.figure(figsize=(50, 50))

        df_beta_temp_d, df_per_temp_d = df_beta_temp.loc[:, date].dropna(axis=0), df_per_temp.loc[:, date].dropna(axis=0)
        idx_beta, idx_per = self.not_outliers_iqr(df_beta_temp_d), self.not_outliers_iqr(df_per_temp_d)
        df_beta_d = pd.DataFrame(df_beta_temp_d.iloc[idx_beta])
        df_per_d = pd.DataFrame(df_per_temp_d.iloc[idx_per])

        df_plot = pd.concat([df_beta_d.T, df_per_d.T], join='inner')
        df_plot.index = ['beta', 'per']
        df_plot = df_plot.T.join(df_clu['cluster']).T
        df_plot = df_plot.dropna(axis=1)

        df_temp = df_plot.T.iloc[:, 0:2].copy()
        self.df_temp, self.df_plot = df_temp, df_plot

        ax.scatter(df_plot.T['beta'], df_plot.T['per'], c=df_plot.T['cluster'].map(color_map))

        plt.title(date)
        plt.xlabel('BETA')
        plt.ylabel('PER')

        fig.canvas.callbacks.connect('button_press_event', self.onclick)

        plt.show(block=False)


        plt.draw()
        plt.pause(0.5)
        fig.clear()


df_beta = pd.read_pickle("../storage/df_raw_data/df_beta.pkl")
df_per_12 = pd.read_pickle("../storage/df_raw_data/df_per_month_12.pkl")
df_list = pd.read_pickle("../storage/df_raw_data/df_list_song_artist.pkl")
df_genre = pd.read_pickle("../storage/df_raw_data/df_genre.pkl")

model = Word2Vec.load('../storage/word_dictionary/month6.model')
df_list = pd.read_pickle("../storage/df_raw_data/df_list_song_artist.pkl")

# ['국내CCM', '한국영화', '댄스', '포크', '락', '발라드', '트로트', '캐롤', '인디', '소울', '일렉트로니카', '힙합', '팝', '드라마', '전체']

# artist든 genre든 간에 number 맞춰야 함


list_date = sorted(list(set.intersection(set(df_per_12.columns.tolist()), set(df_beta.columns.tolist()))))
# plt.ion()
fig, ax = plt.subplots(1)
plt.xlim(-2, 7)
plt.ylim(0, 80)
# plt.figure(figsize=(50, 50))

for date in list_date:
    date = '2021-11-04'
    df_beta_temp, df_per_temp, color_map, df_clu = BetaPlot().color_artist()
    BetaPlot().plot_artist(date, df_beta_temp, df_per_temp, color_map, df_clu)

    # df_beta_temp, df_per_temp, color_map = BetaPlot().color_genre()
    # BetaPlot().plot_genre(date, df_beta_temp, df_per_temp, color_map)

    plt.pause(0.3)
    plt.close()

