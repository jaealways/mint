import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

class PER:
    def per_duration(self, duration, df_price, df_copyright):
        list_month, list_song = df_copyright.columns, df_copyright.index

        array_copyright = df_copyright.to_numpy()
        copyright_mean = np.zeros((array_copyright.shape[0], array_copyright.shape[1] - duration + 1))

        for idx, val in enumerate(array_copyright.T[duration - 1:, :]):
            copyright_mean[:, idx] = np.nanmean(array_copyright[:, idx: idx + duration], axis=1)

        df_copyright_mean = pd.DataFrame(copyright_mean)
        df_copyright_mean.columns, df_copyright_mean.index = list_month[duration - 1:], list_song

        df_per = df_price.copy()

        for idx_c, val_c in enumerate(df_price.columns):
            date_copyright = datetime.strptime(val_c, "%Y-%m-%d") - relativedelta(months=1)
            date_copyright = date_copyright.strftime("%Y-%m")
            for idx_i, val_i in enumerate(df_price.index):
                try:
                    val_copy = df_copyright_mean.loc[val_i, date_copyright]
                    df_per.loc[val_i, val_c] = df_price.loc[val_i, val_c] / (val_copy * 12)
                except:
                    df_per.loc[val_i, val_c] = np.nan
                    # 510, 617, 639, 717 등 저작권 없음

        df_per.to_pickle("../storage/df_raw_data/df_per_month_%s.pkl" % duration)

        return df_per