from pymongo import MongoClient
import pandas as pd
import pickle

client = MongoClient('localhost', 27017)

db1 = client.music_cow
col1 = db1.music_cow_ratio
col2 = db1.youtube_list
col3 = db1.daily_youtube


def read_db():
    list_db = col1.find({})
    data = []
    df = pd.DataFrame(data, columns=['song_num', 'date', 'price_ratio'])
    print(list_db)
    for x in list_db:
        for n in list(x)[2:]:
            if x[n] > 50:
                result = {'song_num': x['num'], 'date' : n, 'price_ratio' : x[n]}
                print(result)
                df = df.append(result, ignore_index=True)
            else:
                pass
    print(df)
    df.to_pickle('df.pkl')


def make_list():
    df = pd.read_pickle('df.pkl')
    df1 = df
    data = []
    df_temp = pd.DataFrame(data)
    for num in range(0, len(df)):
        song_num = df['song_num'][num]
        list_song_info = col2.find({'num': song_num})
        for x in list_song_info:
            video_list = {}

            for num_video in range(1, len(x)-3):
                video_num = x['video{0}'.format(num_video)]['video_num']
                video_list['video{0}'.format(num_video)] = video_num

            print(video_list)
            df_temp = df_temp.append(video_list, ignore_index=True)

    df1 = pd.concat([df1, df_temp], axis=1)
    print(df1)
    df1.to_pickle('df1.pkl')


def make_plot():
    df = pd.read_pickle('df.pkl')
    df1 = pd.read_pickle('df1.pkl')
    for num in range(0, len(df1)):
        for vid_num in range(3, 13):
            youtube_num = int(df1[num][vid_num])
            print(youtube_num)
            video_data = col3.find({'video_num': youtube_num})
            for x in video_data:
                print(x)
                print('ds')



# type(x[n])==int and
read_db()
make_list()
# make_plot()
