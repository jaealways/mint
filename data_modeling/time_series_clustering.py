import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tqdm import tqdm
from tensorflow import keras
from minisom import MiniSom

from data_transformation.db_env import DbEnv, db


class TimeSeriesClustering:
    def __init__(self, str_date, end_date, model):
        self.str_date, self.end_date, self.model = str_date, end_date, model

    def plot_result_som(self, df, cluster_n, df_trend='False'):
        import math
        som_x = som_y = math.ceil(math.sqrt(cluster_n))
        list_index = list(df.index)

        df = df.dropna(axis=1)
        array = df.to_numpy()
        win_map, df_cluster = self.make_cluster_minisom(array, som_x, som_y, list_index)
        self.plot_som_series_averaged_center(som_x, som_y, win_map, df_trend=df_trend)
        df_cluster_dis = self.plot_som_cluster_distribution(som_x, som_y, win_map)

        return df_cluster_dis, df_cluster

    def plot_som_series_averaged_center(self, som_x, som_y, win_map, df_trend):
        fig, axs = plt.subplots(som_x, som_y, figsize=(25, 25))
        fig.suptitle('Clusters')
        for x in tqdm(range(som_x)):
            for y in range(som_y):
                cluster = (x, y)
                if cluster in win_map.keys():
                    for series in win_map[cluster]:
                        axs[cluster].plot(series, c="gray", alpha=0.5)
                    if str(type(df_trend)) == "<class 'pandas.core.frame.DataFrame'>":
                        axs[cluster].plot(df_trend, c="red")
                    axs[cluster].plot(np.average(np.vstack(win_map[cluster]), axis=0), c="blue")
                cluster_number = x * som_y + y + 1
                axs[cluster].set_title(f"Cluster {cluster_number}")

        plt.savefig('../storage/plt_ts_clst/plt_clst_%s_%s_%s.png' % (self.model, self.str_date, self.end_date))
        plt.show(block=False)

        plt.pause(3)
        plt.close()

    def plot_som_cluster_distribution(self, som_x, som_y, win_map):
        cluster_c = []
        cluster_n = []
        for x in tqdm(range(som_x)):
            for y in range(som_y):
                cluster = (x, y)
                if cluster in win_map.keys():
                    cluster_c.append(len(win_map[cluster]))
                else:
                    cluster_c.append(0)
                cluster_number = x * som_y + y + 1
                cluster_n.append(f"Cluster {cluster_number}")

        plt.figure(figsize=(25, 5))
        plt.title("Cluster Distribution for SOM")
        plt.bar(cluster_n, cluster_c)

        plt.savefig('../storage/plt_ts_clst/plt_clst_dst_%s_%s_%s.png' % (self.model, self.str_date, self.end_date))
        plt.show(block=False)

        plt.pause(3)
        plt.close()

        df_cluster_dst = pd.DataFrame([cluster_n, cluster_c], index=['clu_name', 'clu_num'])

        df_cluster_dst = np.transpose(df_cluster_dst)
        df_cluster_dst['clu_ratio'] = df_cluster_dst['clu_num']/df_cluster_dst['clu_num'].sum()

        return df_cluster_dst

    def plot_time_series_denoise(self, list_music_num, rand_num_list, plt_dn_list_scaled, name_denoise_list, color_den_list):
        fig, axs = plt.subplots(len(rand_num_list), 1, figsize=(25, 25))
        for idx, x in enumerate(rand_num_list):
            for plt_dn, name_dn, col_dn in zip(plt_dn_list_scaled, name_denoise_list, color_den_list):
                axs[(idx,)].plot(plt_dn[x], c=col_dn, label=f'{name_dn}')
            plt.legend()
            axs[(idx,)].set_title(f"song_num {list_music_num[x]}")
        plt.savefig('../storage/plt_ts_clst/plt_ts_compare_%s_%s_%s.png' % (self.str_date, self.end_date))
        plt.show(block=False)

        plt.pause(3)
        plt.close()


    def make_cluster_minisom(self, array, som_x, som_y, list_index):
        som = MiniSom(som_x, som_y, len(array[0]), sigma=0.3, learning_rate=0.1)

        som.random_weights_init(array)
        som.train(array, 100000)

        cluster_map = []
        for idx in range(len(array)):
            winner_node = som.winner(array[idx])
            cluster_map.append((list_index[idx], f"Cluster {winner_node[0] * som_y + winner_node[1] + 1}"))

        df_cluster = pd.DataFrame(cluster_map, columns=["Series", "Cluster"]).sort_values(by="Cluster").set_index("Series")

        return som.win_map(array), df_cluster

    def decompose_time_series(self, df, model='additive'):
        """model: 'additive', 'multiplicative'"""
        import copy

        df_copy = copy.copy(df)

        # 작업을 위해 index를 시계열 format으로 변환
        from datetime import datetime

        date = []
        for i in df_copy.columns:
            date.append(datetime.strptime(i, '%Y-%m-%d'))
        df_copy.columns = date
        df_t = np.transpose(df_copy)

        from statsmodels.tsa.seasonal import seasonal_decompose

        df_temp = df_t.apply(lambda x: seasonal_decompose(x, model=model))
        df_resid, df_trend, df_seasonal = df_temp.apply(lambda x: x.resid), df_temp.apply(lambda x: x.trend), df_temp.apply(lambda x: x.seasonal)

        #?? 같은 동작 3번 수행. 간단하게 가능??
        df_resid, df_trend, df_seasonal = df_resid.dropna(axis=1), df_trend.dropna(axis=1), df_seasonal.dropna(axis=1)

        return df_resid, df_trend, df_seasonal

    def make_df_clst_arst_song(self, col, colunms, df_cluster):
        df_clst_artist_song = pd.DataFrame([], index=colunms)
        conn = DbEnv().connect_mongo('music_cow', col)
        list_clst = df_cluster.Cluster.drop_duplicates().values.tolist()
        for cl_n in list_clst:
            index_cl_n = df_cluster[df_cluster['Cluster'] == cl_n].index.tolist()
            print('%s 번 클러스터 - %s' % (cl_n, index_cl_n))
            for song_num in index_cl_n:
                list_song = conn.find({'num': int(song_num)})
                for x in list_song:
                    print('title:', x['song_title'], '---', 'artist:', x['song_artist'])
                    df_temp = pd.DataFrame([x['song_title'], x['song_artist'], cl_n, song_num], index=colunms)
                    df_clst_artist_song = pd.concat([df_clst_artist_song, df_temp], axis=1)

        df_clst_artist_song = np.transpose(df_clst_artist_song)
        df_clst_artist_song.to_pickle("../storage/df_ts_clst/df_clst_artist_song_%s_%s_%s.pkl" % (self.model, self.str_date, self.end_date))

        return df_clst_artist_song
#
#
# np.random.seed(813306)
#
# def build_resnet(input_shape, n_feature_maps, nb_classes):
#     print('build conv_x')
#     x = keras.layers.Input(shape=(input_shape))
#     conv_x = keras.layers.BatchNormalization()(x)
#     conv_x = keras.layers.Conv2D(n_feature_maps, 8, 1, padding='same')(conv_x)
#     conv_x = keras.layers.BatchNormalization()(conv_x)
#     conv_x = keras.layers.Activation('relu')(conv_x)
#
#     print('build conv_y')
#     conv_y = keras.layers.Conv2D(n_feature_maps, 5, 1, padding='same')(conv_x)
#     conv_y = keras.layers.BatchNormalization()(conv_y)
#     conv_y = keras.layers.Activation('relu')(conv_y)
#
#     print('build conv_z')
#     conv_z = keras.layers.Conv2D(n_feature_maps, 3, 1, padding='same')(conv_y)
#     conv_z = keras.layers.BatchNormalization()(conv_z)
#
#     is_expand_channels = not (input_shape[-1] == n_feature_maps)
#     if is_expand_channels:
#         shortcut_y = keras.layers.Conv2D(n_feature_maps, 1, 1, padding='same')(x)
#         shortcut_y = keras.layers.BatchNormalization()(shortcut_y)
#     else:
#         shortcut_y = keras.layers.BatchNormalization()(x)
#     print('Merging skip connection')
#     y = keras.layers.Add()([shortcut_y, conv_z])
#     y = keras.layers.Activation('relu')(y)
#
#     print('build conv_x')
#     x1 = y
#     conv_x = keras.layers.Conv2D(n_feature_maps * 2, 8, 1, padding='same')(x1)
#     conv_x = keras.layers.BatchNormalization()(conv_x)
#     conv_x = keras.layers.Activation('relu')(conv_x)
#
#     print('build conv_y')
#     conv_y = keras.layers.Conv2D(n_feature_maps * 2, 5, 1, padding='same')(conv_x)
#     conv_y = keras.layers.BatchNormalization()(conv_y)
#     conv_y = keras.layers.Activation('relu')(conv_y)
#
#     print('build conv_z')
#     conv_z = keras.layers.Conv2D(n_feature_maps * 2, 3, 1, padding='same')(conv_y)
#     conv_z = keras.layers.BatchNormalization()(conv_z)
#
#     is_expand_channels = not (input_shape[-1] == n_feature_maps * 2)
#     if is_expand_channels:
#         shortcut_y = keras.layers.Conv2D(n_feature_maps * 2, 1, 1, padding='same')(x1)
#         shortcut_y = keras.layers.BatchNormalization()(shortcut_y)
#     else:
#         shortcut_y = keras.layers.BatchNormalization()(x1)
#     print('Merging skip connection')
#     y = keras.layers.Add()([shortcut_y, conv_z])
#     y = keras.layers.Activation('relu')(y)
#
#     print('build conv_x')
#     x1 = y
#     conv_x = keras.layers.Conv2D(n_feature_maps * 2, 8, 1, padding='same')(x1)
#     conv_x = keras.layers.BatchNormalization()(conv_x)
#     conv_x = keras.layers.Activation('relu')(conv_x)
#
#     print('build conv_y')
#     conv_y = keras.layers.Conv2D(n_feature_maps * 2, 5, 1, padding='same')(conv_x)
#     conv_y = keras.layers.BatchNormalization()(conv_y)
#     conv_y = keras.layers.Activation('relu')(conv_y)
#
#     print('build conv_z')
#     conv_z = keras.layers.Conv2D(n_feature_maps * 2, 3, 1, padding='same')(conv_y)
#     conv_z = keras.layers.BatchNormalization()(conv_z)
#
#     is_expand_channels = not (input_shape[-1] == n_feature_maps * 2)
#     if is_expand_channels:
#         shortcut_y = keras.layers.Conv2D(n_feature_maps * 2, 1, 1, padding='same')(x1)
#         shortcut_y = keras.layers.BatchNormalization()(shortcut_y)
#     else:
#         shortcut_y = keras.layers.BatchNormalization()(x1)
#     print('Merging skip connection')
#     y = keras.layers.Add()([shortcut_y, conv_z])
#     y = keras.layers.Activation('relu')(y)
#
#     full = keras.layers.GlobalAveragePooling2D()(y)
#     out = keras.layers.Dense(nb_classes, activation='softmax')(full)
#     print('        -- model was built.')
#     return x, out
#
#
# def readucr(filename):
#     data = np.loadtxt(filename, delimiter=',')
#     Y = data[:, 0]
#     X = data[:, 1:]
#     return X, Y


# nb_epochs = 50
#
# flist = ['Adiac']
# for each in flist:
#     fname = each
#     x_train, y_train = readucr(fname + '/' + fname + '_TRAIN')
#     x_test, y_test = readucr(fname + '/' + fname + '_TEST')
#     nb_classes = len(np.unique(y_test))
#     batch_size = min(x_train.shape[0] / 10, 16)
#
#     y_train = (y_train - y_train.min()) / (y_train.max() - y_train.min()) * (nb_classes - 1)
#     y_test = (y_test - y_test.min()) / (y_test.max() - y_test.min()) * (nb_classes - 1)
#
#     Y_train = keras.utils.to_categorical(y_train, nb_classes)
#     Y_test = keras.utils.to_categorical(y_test, nb_classes)
#
#     x_train_mean = x_train.mean()
#     x_train_std = x_train.std()
#     x_train = (x_train - x_train_mean) / (x_train_std)
#
#     x_test = (x_test - x_train_mean) / (x_train_std)
#     x_train = x_train.reshape(x_train.shape + (1, 1,))
#     x_test = x_test.reshape(x_test.shape + (1, 1,))
#
#     x, y = build_resnet(x_train.shape[1:], 64, nb_classes)
#     model = keras.models.Model(inputs=x, outputs=y)
#     optimizer = keras.optimizers.Adam()
#     model.compile(loss='categorical_crossentropy',
#                   optimizer=optimizer,
#                   metrics=['accuracy'])
#
#     reduce_lr = keras.callbacks.ReduceLROnPlateau(monitor='loss', factor=0.5,
#                                                   patience=50, min_lr=0.0001)
#     hist = model.fit(x_train, Y_train, batch_size=batch_size, epochs=nb_epochs,
#                      verbose=1, validation_data=(x_test, Y_test), callbacks=[reduce_lr])
#     log = pd.DataFrame(hist.history)
#     print(log.loc[log['loss'].idxmin]['loss'], log.loc[log['loss'].idxmin]['val_acc'])
#


# https://github.com/tensorpack/tensorpack/tree/master/examples/ResNet
# https://github.com/cauchyturing/UCR_Time_Series_Classification_Deep_Learning_Baseline
# https://github.com/tensorpack/tensorpack/blob/master/examples/Saliency/CAM-resnet.py