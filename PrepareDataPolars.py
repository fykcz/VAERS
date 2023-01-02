"""
Příprava dat VAERS.
"""

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

#import pandas as pd
import polars as pl
import os

# Načtení základních dat VAERS, jejich prvotní zpracování a uložení do mezisouborů k dalšímu zpracování.
# nastavim si zakladni promenny, odkud a jaky data budu cist
current_path = os.getcwd()
path = os.path.join(current_path, 'data')
print(path)
files = os.listdir(path)
vaxfiles = [f for f in files if f[-7:] == 'VAX.csv']
datafiles = [f for f in files if f[-8:] == 'DATA.csv']
vaxfiles.sort(reverse=False)
datafiles.sort(reverse=False)


# nactu data z VAX souboru a vytvorim jeden obrovskej df
vaxes = pl.DataFrame()

for f in vaxfiles:
    fname = os.path.join(current_path, os.path.join('data', f))
    rok = int(f[:4])
    with open(fname, 'r', encoding='cp850') as fh:
        data = pl.read_csv(fh.read().encode('utf-8')).with_column(pl.lit(rok).alias('fileyear'))
    vaxes = pl.concat([vaxes, data], how='vertical')

# nactu data z DATA souboru a vytvorim jeden obrovskej df
datas = pl.DataFrame()

for f in datafiles:
    fname = os.path.join(current_path, os.path.join('data', f))
    rok = int(f[:4])
    with open(fname, 'r', encoding='cp850') as fh:
        data = pl.read_csv(fh.read().encode('utf-8'))
    
    xda = data.with_columns([pl.lit(rok).alias('fileyear'),
                       pl.col("RECVDATE").str.strptime(pl.Datetime, fmt="%m/%d/%Y").cast(pl.Datetime),
                       pl.col("VAX_DATE").str.strptime(pl.Datetime, fmt="%m/%d/%Y").cast(pl.Datetime),
                       pl.col('AGE_YRS').cast(pl.Float32),
                       pl.col('HOSPDAYS').cast(pl.Int64),
                       pl.col('CAGE_YR').cast(pl.Int64),
                       pl.col('CAGE_MO').cast(pl.Float32)
                       ])
    datas = pl.concat([datas, xda], how='vertical')

# sloucim oba df do jednoho, abych mel ke kazdymu pripadu odpovidajici vax data
bigdata = vaxes.join(datas,
                     left_on='VAERS_ID',
                     right_on='VAERS_ID',
                     how='left').select(['VAERS_ID', 'RECVDATE', 'AGE_YRS', 'SEX', 'DIED', 'VAX_DATE', 'NUMDAYS',
                                         'VAX_TYPE', 'VAX_MANU', 'VAX_LOT', 'VAX_DOSE_SERIES', 'VAX_NAME', (pl.col('VAX_DATE').dt.year()).alias('VAXYEAR')])


# nactu rozdelovaci data, kde jsou rozdeleny vaxs podle nemoci
vaxd = pl.read_excel('vaxgroups_with_disease.xlsx')
disease_by_vax = vaxd.with_columns([
    pl.col('DISEASE').apply(lambda x: pl.Series(x.split(', '))).alias('DIS')
    ]).explode(pl.col('DIS')).select(['VAX_TYPE', 'VAX_GROUP', pl.col('DIS').alias('DISEASE')])

# export dat do mezisouboru
bigdata.write_parquet('bigdata.parquet')
disease_by_vax.write_parquet('disease_by_vax.parquet')
vaxes.write_parquet('vaxes.parquet')
