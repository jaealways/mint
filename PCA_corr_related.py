import seaborn as sns
import pandas as pd
from sklearn.decomposition import PCA
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
import numpy as np
from sklearn.cluster import KMeans

# 814 x 814 correlation 데이터 프레임 읽어오기
a = pd.read_pickle('df_corr.pkl')

# PCA 변환 데이터 반환
pca = PCA(n_components =3)
pca_transformed=pca.fit_transform(a.fillna(0))  # 3차원으로 차원축소 데이터 + 데이터프레임 a의 nan 을 0으로 바꿈

# PCA 변환된 데이터의 컬럼명을 각각 xs, ys, zs 로 명명
xs,ys,zs = pca_transformed[:,0], pca_transformed[:,1], pca_transformed[:,2]


n=783
cmin, cmax = 0, 2
color = np.array([(cmax - cmin) * np.random.random_sample() + cmin for i in range(n)])
plt.rcParams["figure.figsize"] = (6, 6)
fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')
# ax.scatter(xs, ys, zs, c=color, marker='o', s=15, cmap='Greens')   # 색깔 안 나눠짐
# plt.show()


kmeans = KMeans(3)
KM_label = kmeans.fit_predict(pca_transformed)
# ax.scatter(xs, ys, zs, c = KM_label, marker='o', s=15, cmap='Greens')   # 색깔 나눠짐
# plt.show()
# np.where(KM_label == 1)


b = list(np.where(KM_label == 1))
d = list(a.columns)   # a의 columns : 26, 27, 28 ... : song_num

for n in list(b[0]):
    print(d[n])        # b 와 song_num 겹치는 거 출력.



