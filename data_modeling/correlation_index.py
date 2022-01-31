import sys

sys.path.append("..")

import pandas as pd
import numpy as np


class Beta:
    def beta_index(self, duration, df_mcpi, df_price):
        date = df_mcpi.columns
        df_price_temp = df_price.loc[:, date]

        df_mcpi_temp = df_mcpi.iloc[:, -duration - 1:]
        df_price_temp = df_price_temp.iloc[:, -duration - 1:].dropna(axis='index')
        df_corr, df_std = pd.DataFrame(df_price_temp.iloc[:, -1].copy()), pd.DataFrame(df_price_temp.iloc[:, -1].copy())

        array_return_mcpi = np.true_divide(np.diff(df_mcpi_temp.to_numpy()), df_mcpi_temp.to_numpy()[:, 1:])
        array_return_price = np.true_divide(np.diff(df_price_temp.to_numpy()), df_price_temp.to_numpy()[:, 1:])

        for idx, row in enumerate(array_return_price):
            df_corr.iloc[idx] = np.corrcoef(array_return_mcpi, row)[0, 1]
            df_std.iloc[idx] = np.std(row)

        df_beta = df_corr.mul(df_std / np.std(array_return_mcpi))
        df_beta = df_beta.fillna(0)
        df_beta.to_pickle("../storage/df_raw_data/df_beta.pkl")

        return df_beta
