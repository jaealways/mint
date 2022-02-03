import sys

sys.path.append("..")

import pandas as pd
import numpy as np


class Beta:
    def beta_index(self, duration, df_mcpi, df_price):
        list_date, list_song = df_mcpi.columns.tolist(), df_price.index.tolist()
        df_price_temp = df_price.loc[:, list_date]
        df_mcpi_temp = df_mcpi.loc[:, list_date]
        list_date = df_mcpi_temp.columns.tolist()

        array_mcpi, array_price = df_mcpi_temp.to_numpy(), df_price_temp.to_numpy()
        array_corr, array_std, array_beta = array_price[:, duration:].copy(), array_price[:, duration:].copy(), array_price[:, duration:].copy()
        array_diff_mcpi, array_diff_price = np.diff(array_mcpi), np.diff(array_price)

        array_return_mcpi = np.true_divide(array_diff_mcpi, array_mcpi[:, 1:])
        array_return_price = np.true_divide(array_diff_price, array_price[:, 1:])

        for idx in range(array_return_price.shape[1] - duration+1):
            array_corr[:, idx] = np.corrcoef(array_return_mcpi[:, idx:idx+duration][0], array_return_price[:, idx:idx+duration])[0, 1:]
            array_std[:, idx] = np.std(array_return_price[:, idx:idx+duration], axis=1)
            array_beta[:, idx] = np.multiply(array_corr[:, idx], array_std[:, idx] / np.std(array_return_mcpi[0, idx:idx+duration]))

        df_beta = pd.DataFrame(array_beta)
        df_beta.columns, df_beta.index = list_date[duration:], list_song
        df_beta.to_pickle("../storage/df_raw_data/df_beta.pkl")

        return df_beta


# duration = 365
# df_mcpi = pd.read_pickle("../storage/df_raw_data/df_mcpi_17-01-01_23-12-31.pkl")
# df_price = pd.read_pickle("../storage/df_raw_data/df_price_17-01-01_23-12-31.pkl")
#
# Beta().beta_index(duration, df_mcpi, df_price)

