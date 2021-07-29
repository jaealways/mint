from pymongo import MongoClient
import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
import math
import pickle
import matplotlib.pyplot as plt

client = MongoClient('localhost', 27017)

db1 = client.music_cow
col1 = db1.music_list_split
col2 = db1.daily_youtube
col3 = db1.daily_genie
col4 = db1.daily_music_cow

# music_list_split 에서 곡별로 하나씩 끊어서


def df_list():
    list_db = col1.find({}, {'num': {"$slice": [1, 1]}})
    data = []
    df = pd.DataFrame(data, columns=['num', 'song_artist', 'song_title'])
    for x in list_db:
        if 'song_artist_main_kor1' in x['list_split']:
            song_artist = x['list_split']['song_artist_main_kor1']
        else:
            song_artist = x['list_split']['song_artist_main_eng1']
        if 'song_title_main_kor' in x['list_split']:
            song_title = x['list_split']['song_title_main_kor']
        else:
            song_title = x['list_split']['song_title_main_eng']
        result = {'num': x['num'], 'song_artist': song_artist, 'song_title': song_title}
        print(result)
        df = df.append(result, ignore_index=True)
    print(df)
    df.to_pickle('df_song_list.pkl')


def add_num():
    df = pd.read_pickle('df_song_list.pkl')
    for n in range(0, len(df)):
        num, song_artist, song_title = df['num'][n], df['song_artist'][n], df['song_title'][n]
        print(num, song_artist, song_title)
        list_youtube = col2.find({"$and": [{"song_artist": song_artist}, {"song_title": song_title}]})
        for x in list_youtube:
            print(x)
            if 'num' in x.keys():
                continue
            list_song_x = {"song_artist": x['song_artist'], "song_title": x['song_title']}
            col2.update_many(list_song_x, {'$set': {'num': num}}, upsert=True)
        list_genie = col3.find({"$and": [{"song_artist": song_artist}, {"song_title": song_title}]})
        for y in list_genie:
            print(y)
            if 'num' in y.keys():
                continue
            list_song_y = {"song_artist": y['song_artist'], "song_title": y['song_title']}
            col3.update_many(list_song_y, {'$set': {'num': num}}, upsert=True)


def make_df():
    df = pd.read_pickle('df_song_list.pkl')
    df_all = []
    df_cow_all, df_you_all, df_gen_all = pd.DataFrame(df_all), pd.DataFrame(df_all), pd.DataFrame(df_all)
    for num in df['num']:
        # num = 491
        data = []
        df_youtube = pd.DataFrame(data)
        list_youtube = col2.find({'num': num})
        for x in list_youtube:
            result_you = {'vid_num': x['video_num']}
            for n in list(x)[6:]:
                if n == 'num':
                    continue
                result_you[n] = x[n]['viewCount']
            df_youtube = df_youtube.append(result_you, ignore_index=True)
        df_genie = pd.DataFrame(data)
        list_genie = col3.find({'num': num})
        for y in list_genie:
            result_gen = {'link': y['link'].split('xgnm=')[1]}
            for n in list(y)[8:]:
                if n == 'num':
                    continue
                result_gen[n] = y[n]['total_play']
            df_genie = df_genie.append(result_gen, ignore_index=True)
        df_cow = pd.DataFrame(data)
        list_cow = col4.find({'num': num})
        for z in list_cow:
            result_cow = {'num': z['num']}
            for n in list(z)[4:]:
                result_cow[n] = z[n]['price']
            df_cow = df_cow.append(result_cow, ignore_index=True)

        df_you_col = sorted(list(df_youtube.columns))
        df_index_youtube = pd.DataFrame(data)
        for date in df_you_col[1:-1]:
            num_date = df_you_col.index(date)
            date_yesterday = df_you_col[num_date - 1]
            na_delete_you = pd.notna(df_youtube[date]) * pd.notna(df_youtube[date_yesterday])
            date_log = [int(a) for a in (na_delete_you * df_youtube[date]).dropna() if a != '']
            yesterday_log = [int(b) for b in (na_delete_you * df_youtube[date_yesterday]).dropna() if b != '']
            for index, n in enumerate(date_log):
                if n == 0:
                    del date_log[index], yesterday_log[index]
            for index, n in enumerate(yesterday_log):
                if n == 0:
                    del date_log[index], yesterday_log[index]
            # date log와 yesteday log가 넘버가 일치하지 않음, vid_num이 포함되면서
            # date_log, yesterday_log = list(map(int, df_youtube[date])), list(map(int, df_youtube[date_yesterday]))
            data_log = np.log(date_log) - np.log(yesterday_log)
            if len(data_log) < 2:
                continue
            df_index_youtube[date] = [sum(data_log)/len(data_log)]
        df_index_youtube = df_index_youtube.rename(index={0: num})

        df_gen_col = sorted(list(df_genie.columns))
        df_index_genie = pd.DataFrame(data)
        for date in df_gen_col[1:-1]:
            num_date = df_gen_col.index(date)
            date_yesterday = df_gen_col[num_date - 1]
            na_delete_gen = pd.notna(df_genie[date]) * pd.notna(df_genie[date_yesterday])
            date_log = [int(c) for c in (na_delete_gen * df_genie[date]).dropna() if c != '']
            yesterday_log = [int(d) for d in (na_delete_gen * df_genie[date_yesterday]).dropna() if d != '']
            for index, n in enumerate(date_log):
                if n == 0:
                    del date_log[index], yesterday_log[index]
            for index, n in enumerate(yesterday_log):
                if n == 0:
                    del date_log[index], yesterday_log[index]
            data_log = np.log(date_log) - np.log(yesterday_log)
            if len(data_log) < 2:
                continue
            df_index_genie[date] = [sum(data_log)/len(data_log)]
        df_index_genie = df_index_genie.rename(index={0: num})

        # 뮤카도 로그
        df_cow_col = list(df_cow.columns)
        df_index_cow = pd.DataFrame(data)
        for date in df_cow_col[1:-1]:
            num_date = df_cow_col.index(date)
            date_yesterday = df_cow_col[num_date - 1]
            na_delete_cow = pd.notna(df_cow[date]) * pd.notna(df_cow[date_yesterday])
            date_log = [int(e) for e in (na_delete_cow * df_cow[date]).dropna() if e != '']
            yesterday_log = [int(f) for f in (na_delete_cow * df_cow[date_yesterday]).dropna() if f != '']
            data_log = np.log(date_log) - np.log(yesterday_log)
            df_index_cow[date] = [sum(data_log)/len(data_log)]
        df_index_cow = df_index_cow.rename(index={0: num})

        print('%s번 곡' % num)
        print(df_index_cow)
        print(df_index_youtube)
        print(df_index_genie)

        # columns_index = df_cow_col[:-1]
        # columns_index.extend(x for x in df_gen_col[:-1] if x not in columns_index)
        # columns_index.extend(x for x in df_you_col[:-1] if x not in columns_index)
        # columns_index = sorted(columns_index)
        # plt.plot(sorted(df_index_youtube.columns), df_index_youtube.loc[0,:], label='youtube')
        # plt.plot(sorted(df_index_genie.columns), df_index_genie.loc[0,:], label='genie')
        # plt.plot(sorted(df_index_cow.columns), df_index_cow.loc[0,:], label='music_cow')
        # plt.title('song_num {0}'.format(num))
        # plt.legend(loc='upper right')
        # plt.xticks(ticks=range(1, len(columns_index)), labels=columns_index[1:], rotation=90)
        # plt.show()

        df_cow_all = df_cow_all.append(df_index_cow)
        df_gen_all = df_gen_all.append(df_index_genie)
        df_you_all = df_you_all.append(df_index_youtube)

    df_cow_all.to_pickle('df_cow_all.pkl')
    df_gen_all.to_pickle('df_gen_all.pkl')
    df_you_all.to_pickle('df_you_all.pkl')

def get_index_corr():
    df_cow_all = pd.read_pickle('df_cow_all.pkl')
    df_gen_all = pd.read_pickle('df_gen_all.pkl')
    df_you_all = pd.read_pickle('df_you_all.pkl')

    # print('+cow+')
    # for col in range(len(df_cow_all)):
    #     for row in range(len(df_cow_all.columns)):
    #         price_cow = df_cow_all.iloc[col, row]
    #         if price_cow > 0.7:
    #             print(col, row, price_cow)

    print('+you+')
    for col in range(len(df_you_all)):
        for row in range(len(df_you_all.columns)):
            price_you = df_you_all.iloc[col, row]
            if price_you > 0.03:
                print(list(df_you_all.columns)[row], list(df_you_all.index)[col], price_you)

    print('+gen+')
    for col in range(len(df_gen_all)):
        for row in range(len(df_gen_all.columns)):
            price_gen = df_gen_all.iloc[col, row]
            if price_gen > 0.03:
                print(list(df_gen_all.columns)[row], list(df_gen_all.index)[col], price_gen)

    print('test')


# df_list()
# add_num()
# make_df()
get_index_corr()
