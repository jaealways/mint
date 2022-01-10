import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from statsmodels.tsa.api import SimpleExpSmoothing, Holt
from pywt import wavedec, waverec
import pywt
from statsmodels.robust import mad
import tensorflow as tf
from tensorflow import keras


class DataPreprocess:
    def make_df_nan_same(self, df_a, df_b, col):
        """df_a의 col 값을 기준으로, df_b 통합하여 결측치 제거"""
        df_a = df_a.reset_index()
        df_b = df_b.reset_index()

        df_a = pd.merge(df_a, df_b, how='inner', on=col)
        df_a = df_a.drop_duplicates()
        df_a = df_a.set_index(col)

        df_a_droped = df_a.dropna(axis=0)
        df_a_droped = df_a_droped.sort_index(ascending=True)
        list_a_droped = list(df_a_droped.columns)

        df_b_droped = pd.DataFrame(df_a_droped[df_a_droped.columns[-(len(df_b.columns)-1)]])
        df_a_droped = pd.DataFrame(df_a_droped.drop(df_a_droped.columns[-(len(df_b.columns)-1)], axis='columns'))

        return df_a_droped, df_b_droped, list_a_droped

    def scale_df(self, df, method='Standard'):
        list_df_droped = df.columns
        if method == 'MinMax':
            array_scaled = MinMaxScaler().fit_transform(df)
        else:
            array_scaled = StandardScaler().fit_transform(df)
        df_scaled = pd.DataFrame(array_scaled)
        df_scaled.columns = list_df_droped
        df_scaled.index = df.index

        return df_scaled

    def scale_array(self, array, method='Standard'):
        if method == 'MinMax':
            array_scaled = MinMaxScaler(feature_range=(0.01, 1),).fit_transform(array)
        else:
            array_scaled = StandardScaler().fit_transform(array)

        return array_scaled

    def smooth_time_series(self, df_time, method="exp", level=0.3):
        """
        <method>
        exp: SimpleExp,
        holt: holt_winter
        wave: wavelet
        sae: Stacked Auto-encoder
        """
        array_time = df_time.to_numpy()
        array_time_t = np.transpose(array_time)

        if method == "exp":
            list_time_smooth_t = [SimpleExpSmoothing(x).fit(smoothing_level=level).fittedvalues for x in array_time_t]
        elif method == "holt":
            list_time_smooth_t = [Holt(x).fit(smoothing_level=level).fittedvalues for x in array_time_t]
        elif method == "wave":
            list_time_smooth_t = []
            for x in array_time_t:
                coeff = pywt.wavedec(x, "haar", mode='periodization', level=level, axis=0)
                sigma = mad(coeff[-level])
                uthresh = sigma * np.sqrt(2 * np.log(len(x)))
                coeff[1:] = (pywt.threshold(i, value=uthresh, mode="hard") for i in coeff[1:])
                list_time_smooth_t.append(pywt.waverec(coeff, "haar", mode='periodization', axis=0))
        elif method == "sae":
            list_time_smooth_t = []
            for x in array_time_t:
                input_dim = x.shape[0]
                hidden_dim = 10

                # Layer 1 - Input Layer, SAE_1
                input_data = keras.layers.Input(shape=(input_dim,))
                encoded = keras.layers.Dense(hidden_dim, activation='sigmoid')(input_data)
                decoded = keras.layers.Dense(input_dim, activation='sigmoid')(encoded)
                # Layer 2 - SAE_2
                encoded = keras.layers.Dense(hidden_dim, activation='sigmoid')(decoded)
                decoded = keras.layers.Dense(input_dim, activation='sigmoid')(encoded)
                # Layer 3 - SAE_3
                encoded = keras.layers.Dense(hidden_dim, activation='sigmoid')(decoded)
                decoded = keras.layers.Dense(input_dim, activation='sigmoid')(encoded)
                # Layer 4 - SAE_4
                encoded = keras.layers.Dense(hidden_dim, activation='sigmoid')(decoded)
                decoded = keras.layers.Dense(input_dim, activation='sigmoid')(encoded)
                # Layer 5 - SAE_5
                encoded = keras.layers.Dense(hidden_dim, activation='sigmoid')(decoded)
                decoded = keras.layers.Dense(input_dim, activation='sigmoid')(encoded)

                autoencoder = keras.models.Model(input_data, decoded)
                autoencoder.compile(optimizer='sgd', loss='mse')
                autoencoder.summary()

                autoencoder.fit(x[0], x[0], epochs=10, verbose=2)
                autoencoded_data = autoencoder.predict(x[0])
                list_time_smooth_t.append(autoencoded_data)

        array_time_smooth = np.array(list_time_smooth_t).transpose()
        df_time_smooth = pd.DataFrame(array_time_smooth)
        df_time_smooth.columns = df_time.columns
        df_time_smooth.index = df_time.index

        return df_time_smooth


# df_price = pd.read_pickle('../storage/df_price.pkl')
# df_mcpi = pd.read_pickle('../storage/df_mcpi.pkl')
# df_price_droped, df_mcpi_droped, list_price_droped = DataPreprocess().make_df_nan_same(df_price, df_mcpi, 'date')
# array_price_sae = DataPreprocess().smooth_time_series(df_time=df_price_droped, method="sae", level=2)
# array_price_sae