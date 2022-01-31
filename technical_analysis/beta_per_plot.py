import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


df_beta = pd.read_pickle("../storage/df_raw_data/df_beta.pkl")
df_per_12 = pd.read_pickle("../storage/df_raw_data/df_per_month_12.pkl")
df_list = pd.read_pickle("../storage/df_raw_data/df_list_song_artist.pkl")

df_per = pd.DataFrame(df_per_12.iloc[:, -1]).dropna(axis=0)

def not_outliers_iqr(data):
    q1, q3 = np.percentile(data, [25, 75])
    iqr = q3 - q1
    ub, lb = q1 + (1.5 * iqr), q1 - (1.5 * iqr)

    return np.where((data < ub) & (data >= lb))

idx_beta, idx_per = not_outliers_iqr(df_beta)[0], not_outliers_iqr(df_per)[0]
df_beta = df_beta.iloc[idx_beta]
df_per = df_per.iloc[idx_per]

df_plot = pd.concat([df_beta.T, df_per.T], join='inner')
df_plot.index = ['beta', 'per']
df_temp = df_plot.T.copy()

fig, ax = plt.subplots(1)

ax.scatter(df_plot.T['beta'], df_plot.T['per'])

def onclick(event):
    df_temp['beta'] = (df_plot.T['beta']-event.xdata).abs() * 10
    df_temp['per'] = (df_temp['per'] - event.ydata).abs()

    song_num = df_plot.T.iloc[df_temp.sum(axis=1).argsort()[:1]].index[0]
    temp = df_list.loc[song_num].tolist()
    title, artist = temp[0], temp[1]
    print('song_num=%s, artist=%s, title=%s beta=%f, per=%f' % (song_num, artist, title, event.xdata, event.ydata))

plt.xlabel('BETA')
plt.ylabel('PER')
fig.canvas.callbacks.connect('button_press_event', onclick)
plt.show()