# %% import package
import os
from pathlib import Path

import pandas as pd


work_dir = str(Path(__name__).resolve().parent)


# %% read files
files_sort = ['provinsi.csv', 'kabupaten_kota.csv', 'kecamatan.csv', 'kelurahan.csv']
file_list = [os.path.join(work_dir, 'csv_files', file_) for file_ in files_sort]


dfs_table = {}
for file_ in file_list:
    file_name = os.path.basename(file_)
    level = file_name.split('.')[0]

    df = (
        pd.read_csv(file_, sep=',', dtype='string')
        .rename(columns={'id':'code'})
    )

    if level == 'provinsi':
        df.insert(0, 'id', range(1, len(df)+1))

    elif level == "kabupaten_kota":
        df[['code_provinsi','code']] = df['code'].str.split('.', expand=True)
        df = (
            (
                dfs_table['provinsi'].drop(columns=['name'])
                .rename(columns={'id':'id_provinsi',
                                 'code':'code_provinsi'})
            ).merge(df, how='right',
                     on='code_provinsi',
                     )
        )
        df.insert(0, 'id', range(1, len(df)+1))

    elif level == "kecamatan":
        df[['code_provinsi','code_kabupaten_kota','code']] = df['code'].str.split('.', expand=True)
        df = (
            (
                dfs_table['kabupaten_kota'].drop(columns=['name', 'id_provinsi'])
                .rename(columns={'id':'id_kabupaten_kota',
                                 'code':'code_kabupaten_kota'})
            ).merge(df, how='right',
                     on=['code_provinsi','code_kabupaten_kota'],
                     )
        )
        df.insert(0, 'id', range(1, len(df)+1))

    elif level == "kelurahan":
        df[['code_provinsi','code_kabupaten_kota','code_kecamatan','code']] = df['code'].str.split('.', expand=True)
        df = (
            (
                dfs_table['kecamatan'].drop(columns=['name', 'id_kabupaten_kota',])
                .rename(columns={'id':'id_kecamatan',
                                 'code':'code_kecamatan'})
            ).merge(df, how='right',
                     on=['code_provinsi','code_kabupaten_kota','code_kecamatan'],
                     )
        )
        df.insert(0, 'id', range(1, len(df)+1))

    dfs_table[file_name.split('.csv')[0]] = df.copy()


os.makedirs('output', exist_ok=True)
for k, v in dfs_table.items():
    drop_cols = v.filter(regex="code_").columns
    dfs_table[k] = v.drop(columns=drop_cols)

    output_file = os.path.join(work_dir, 'output', f'{k}.csv')
    dfs_table[k].to_csv(output_file, index=False, sep=',', quoting=1)

# %%
