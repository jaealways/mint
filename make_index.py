from pymongo import MongoClient
import pandas as pd
import numpy as np
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
            list_song_x = {"song_artist": x['song_artist'], "song_title": x['song_title']}
            col2.update_many(list_song_x, {'$set': {'num': num}}, upsert=True)
        list_genie = col3.find({"$and": [{"song_artist": song_artist}, {"song_title": song_title}]})
        for y in list_genie:
            print(y)
            list_song_y = {"song_artist": y['song_artist'], "song_title": y['song_title']}
            col3.update_many(list_song_y, {'$set': {'num': num}}, upsert=True)


def make_df():
    df = pd.read_pickle('df_song_list.pkl')
    for num in df['num']:
        data = []
        df_youtube = pd.DataFrame(data)
        list_youtube = col2.find({'num': num})
        for x in list_youtube:
            result = {'vid_num': x['video_num']}
            for n in list(x)[6:]:
                if n == 'num':
                    continue
                result[n] = x[n]['viewCount']
            df_youtube = df_youtube.append(result, ignore_index=True)
        df_genie = pd.DataFrame(data)
        list_genie = col3.find({'num': num})
        for y in list_genie:
            result = {'link': y['link'].split('xgnm=')[1]}
            for n in list(y)[8:]:
                if n == 'num':
                    continue
                result[n] = y[n]['total_play']
            df_genie = df_genie.append(result, ignore_index=True)
        df_cow = pd.DataFrame(data)
        list_cow = col4.find({'num': num})
        for z in list_cow:
            result['num'] = z['num']
            for n in list(z)[4:]:
                result[n] = z[n]['price']
            df_cow = df_cow.append(result, ignore_index=True)

        df_you_col = list(df_youtube.columns)
        df_index_youtube = pd.DataFrame(data)
        for date in df_you_col[1:-1]:
            num_date = df_you_col.index(date)
            date_yesterday = df_you_col[num_date - 1]
            na_delete_you = pd.notna(df_youtube[date]) * pd.notna(df_youtube[date_yesterday])
            date_log = [int(a) for a in (na_delete_you * df_youtube[date]).dropna() if a != '']
            yesterday_log = [int(b) for b in (na_delete_you * df_youtube[date_yesterday]).dropna() if b != '']
            # date_log, yesterday_log = list(map(int, df_youtube[date])), list(map(int, df_youtube[date_yesterday]))
            data_log = np.log(date_log) - np.log(yesterday_log)
            df_index_youtube[date] = [sum(data_log)/len(data_log)]

        df_gen_col = list(df_genie.columns)
        df_index_genie = pd.DataFrame(data)
        for date in df_gen_col[1:-1]:
            num_date = df_gen_col.index(date)
            date_yesterday = df_gen_col[num_date - 1]
            na_delete_gen = pd.notna(df_genie[date]) * pd.notna(df_genie[date_yesterday])
            date_log = [int(c) for c in (na_delete_gen * df_genie[date]).dropna() if c != '']
            yesterday_log = [int(d) for d in (na_delete_gen * df_genie[date_yesterday]).dropna() if d != '']
            data_log = np.log(date_log) - np.log(yesterday_log)
            df_index_genie[date] = [sum(data_log)/len(data_log)]

        plt.plot(df_index_youtube.columns, df_index_youtube, df_index_genie.columns, df_index_genie, df_cow.columns, df_cow)
        # plt.plot(df_index_genie.columns, df_index_genie)
        # plt.plot(df_cow.columns, df_cow)
        plt.show()


# add_num()
make_df()

