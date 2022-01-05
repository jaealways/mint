import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tqdm import tqdm


class TimeSeriesClustering:
    def plot_som_series_averaged_center(self, som_x, som_y, win_map, df_trend=False):
        fig, axs = plt.subplots(som_x, som_y, figsize=(25, 25))
        fig.suptitle('Clusters')
        for x in tqdm(range(som_x)):
            for y in range(som_y):
                cluster = (x, y)
                if cluster in win_map.keys():
                    for series in win_map[cluster]:
                        axs[cluster].plot(series, c="gray", alpha=0.5)
                    if df_trend.empty == False:
                        axs[cluster].plot(df_trend, c="red")
                    axs[cluster].plot(np.average(np.vstack(win_map[cluster]), axis=0), c="blue")
                cluster_number = x * som_y + y + 1
                axs[cluster].set_title(f"Cluster {cluster_number}")
        plt.show()

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
        plt.show()

        df_cluster_dst = pd.DataFrame([cluster_n, cluster_c], index=['clu_num', 'clu_name'])
        df_cluster_dst = np.transpose(df_cluster_dst)

        return df_cluster_dst

    def plot_time_series_denoise(self, list_music_num, rand_num_list, plt_dn_list_scaled, name_denoise_list, color_den_list):
        fig, axs = plt.subplots(len(rand_num_list), 1, figsize=(25, 25))
        for idx, x in enumerate(rand_num_list):
            for plt_dn, name_dn, col_dn in zip(plt_dn_list_scaled, name_denoise_list, color_den_list):
                axs[(idx,)].plot(plt_dn[x], c=col_dn, label=f'{name_dn}')
            plt.legend()
            axs[(idx,)].set_title(f"song_num {list_music_num[x]}")
        plt.show()

