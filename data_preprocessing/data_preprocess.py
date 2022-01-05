import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from statsmodels.tsa.api import SimpleExpSmoothing, Holt


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
            array_scaled = MinMaxScaler().fit_transform(array)
        else:
            array_scaled = StandardScaler().fit_transform(array)

        return array_scaled

    def smooth_time_series(self, df_time, method="exp", level=0.3):
        """
        <method>
        exp: SimpleExp,
        holt: holt_winter
        """
        array_time = df_time.to_numpy()
        array_time_t = np.transpose(array_time)

        if method == "exp":
            list_time_smooth_t = [SimpleExpSmoothing(x).fit(smoothing_level=level).fittedvalues for x in array_time_t]
        elif method == "holt":
            list_time_smooth_t = [Holt(x).fit(smoothing_level=level).fittedvalues for x in array_time_t]
        array_time_smooth = np.array(list_time_smooth_t).transpose()

        return array_time_smooth

