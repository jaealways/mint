from pymongo import MongoClient
import pandas as pd
import pickle
import matplotlib.pyplot as plt

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
                result = {'song_num': x['num'], 'date': n, 'price_ratio': x[n]}
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


def to_make_plot():
    df = pd.read_pickle('df.pkl')
    df1 = pd.read_pickle('df1.pkl')
    data = []
    # dataframe에서 행 읽기 [song_num, date, price_ratio, video~]
    for num in range(0, len(df1)+1):
        vec = df1.loc[num]
        date = vec[1]
        for vid_num in range(3, len(vec)+1 - vec.isnull().sum()):
            youtube_num = vec[vid_num]
            video_data = col3.find({'video_num': youtube_num})
            df2 = pd.DataFrame(data, columns=['date', 'viewCount', 'likeCount', 'dislikeCount', 'favoriteCount', 'commentCount'])
            for x in video_data:
                video_info = {'title_video': x['title_video'], 'song_title': x['song_title'], 'song_artist': x['song_artist']}
                for n in list(x)[6:]:
                    result = {'date': n, 'viewCount': x[n]['viewCount'], 'likeCount': x[n]['likeCount'], 'dislikeCount': x[n]['dislikeCount'],
                              'favoriteCount': x[n]['favoriteCount'], 'commentCount': x[n]['commentCount']}
                    df2 = df2.append(result, ignore_index=True)
                df2.set_index('date')
                df2_num = df2.astype({'viewCount':int, 'likeCount':int, 'dislikeCount':int, 'favoriteCount':int, 'commentCount':int})
                print(df2_num)
                df2_num.plot(x='date', y='viewCount')
                plt.show()
                print('70퍼 상승:', date)

            # youtube_num = int(df1[num][vid_num])


# read_db()
# make_list()
to_make_plot()
# make_plot()
