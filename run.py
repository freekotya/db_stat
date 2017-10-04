import sqlite3
import numpy as np
import pandas as pd

from matplotlib import pyplot as plt
from buckets_manager import BucketsManager


def convert_as_query(df):
    return df.to_csv(sep='|', index=False).split("\n")[1:-1]


if __name__ == "__main__":
    cnx = sqlite3.connect('data/stats.db')
    df = pd.read_sql_query("SELECT * FROM statistics", cnx)

    df_part = df[['bucket_time', 'bucket_size', 'bucket_name', 'bucket_value', 'bucket_type']]

    inputs = []
    for bucket_name, df_bucket_name in df_part.groupby('bucket_name'):
        inputs.append(convert_as_query(df_bucket_name))

    bm = BucketsManager()
    bm.process(inputs)

    data = bm.stat(bucket_size=600, start=1506376740, end=1506478920)
    bucket_names = bm.bucket_names()

    grid_width = 4
    grid_height = len(bucket_names) // grid_width
    grid_height += 1 if len(bucket_names) % grid_width != 0 else 0

    grid_shape = (grid_height, grid_width)

    f, axarr = plt.subplots(grid_height, grid_width, figsize=(20, 20))

    for index, name in enumerate(bucket_names):
        i, j = np.unravel_index(index, dims=grid_shape)
        axarr[i, j].set_title(name)
        axarr[i, j].plot(data['buckets'], data['values'][name])
    f.subplots_adjust(hspace=0.3, wspace=0.2)
    plt.show()