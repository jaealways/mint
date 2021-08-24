import seaborn as sns
from pymongo import MongoClient
import pandas as pd
from sklearn.decomposition import PCA
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt

import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score
from sklearn.cluster import KMeans
import re


client = MongoClient('localhost', 27017)

db1 = client.music_cow
col1 = db1.copyright_type
col2 = db1.music_list


class copyrightPCA:
    def __init__(self):
        #self.db_read()
        self.copyright_PCA()


    def db_read(self):

        copyright_type = col1.find({})
        music_list = col2.find({}, {'num': {"$slice": [1, 1]}})

        # stock_num, song_num 모으기
        stock_num = []
        song_num = []
        for a in music_list:
            stock_num.append(int(a['stock_num']))
            song_num.append(a['num'])

        data = []
        df = pd.DataFrame(data)

        # dataframe 으로 만들기
        i = 0
        for x in copyright_type:

            price = x['price_recent_year'][:-1]

            if "," in price:
                price = price.replace(",", "")

            result = {
                      'song_num' : x['num'],
                      'price_recent_year': stock_num[i] * float(price),
                      'broadcast' : float(x['broadcast'][x['broadcast'].find('(')+1:x['broadcast'].find('%')]),
                      'transmission' : float(x['transmission'][x['transmission'].find('(')+1:x['transmission'].find('%')]),
                      'copy' : float(x['copy'][x['copy'].find('(')+1:x['copy'].find('%')]),
                      'performance': float(x['performance'][x['performance'].find('(')+1:x['performance'].find('%')]),
                      'abroad' : float(x['abroad'][x['abroad'].find('(')+1:x['abroad'].find('%')]),
                      'etc' : float(x['etc'][x['etc'].find('(')+1:x['etc'].find('%')])}

            df = df.append(result, ignore_index=True)
            i=i+1

        df = df.set_index('song_num')  # index를 song_num으로

        # df = df.rename(index = song_num)

        df.to_pickle('df_copyright_type.pkl')

        print(df)

    def copyright_PCA(self):
        df = pd.read_pickle('df_copyright_type.pkl')

        # y_target : price_recent_year
        # 속성 6개간 correlation 체크필요

        #y_target = df['price_recent_year']
        X_features = df

        # 속성간 correlation
        corr = X_features.corr()
        plt.figure(figsize=(14,14))
        #sns.heatmap(corr, annot = True, fmt = '.1g')
        # plt.show()



        # 2개의 PCA 속성을 가진 PCA 객체 생성, explained_variance_ratio_ 계산을 위해 fit() 호출
        scaler = StandardScaler()
        df_cols_scaled = scaler.fit_transform(X_features)
        pca = PCA(n_components=3)
        pca.fit(df_cols_scaled)
        # print('PCA별 component별 변동성 : ', pca.explained_variance_ratio_)
        #output - 2차원 : PCA별 component별 변동성 :  [0.35740238 0.23656451]
        #       - 3차원 : PCA별 component별 변동성 :   [0.35740238 0.23656451 0.22325792]


        # PCA
        pca = PCA(n_components=2)

        # fit() 과 transform() 을 호출해 PCA 변환 데이터 변환
        pca.fit(df_cols_scaled)
        copyright_pca = pca.transform(df_cols_scaled)
        # print(copyright_pca.shape) # [output]: (814, 2)


        # # PCA 변환된 데이터의 컬럼명 명명
        # xs, ys = pca_transformed[:, 0], pca_transformed[:, 1]
        pca_columns = ['pca_component_1', 'pca_component_2']
        # copydf_pca = pd.DataFrame(copyright_pca, columns=pca_columns)
        # print(copydf_pca.head(3))
    #     pca_component_1  pca_component_2
    # 0     0.192136        - 0.363935
    # 1   - 0.381794        - 0.379894
    # 2     1.737720        - 0.946942

        # 2차원 상에서 시각화
        # pc_y = np.c_[copyright_pca, y_target]
        pca_df = pd.DataFrame(copyright_pca, columns = ['pca_component_1', 'pca_component_2'])
        # a= pca_df.loc[pca_df[pca_df['pca_component_1']<= 0.1]]
        plt.scatter(x=pca_df['pca_component_1'],y=pca_df['pca_component_2'])
        plt.show()

        print("%%%")
        # n = 814
        # cmin, cmax = 0, 2
        # color = np.array([(cmax - cmin) * np.random.random_sample() + cmin for i in range(n)])
        # plt.rcParams["figure.figsize"] = (6, 6)
        # fig = plt.figure()
        # ax = fig.add_subplot(111, projection='3d')
        # ax.scatter(xs, ys, c=color, marker='o', s=15, cmap='Greens')  # 색깔 안 나눠짐
        # plt.show()


copyrightPCA()






#
