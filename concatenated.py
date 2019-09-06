import os
import pandas as pd
from mystore import *
import time

connection_string = "localhost"
txts_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "txts")

lib = Library(connection_string=connection_string)
lib = lib.get_or_initialize_library("TEST")

dfs_csv = []
for txt in os.listdir(txts_folder):
    filepath = os.path.join(txts_folder, txt)
    df = pd.read_csv(filepath, header=None, sep=" ",
                     parse_dates=["date"], dayfirst=True,
                     names=["date", "open_position_profit", "position", "gap", "underlying_vol",
                            "trade_number"])
    dfs_csv.append(df)

con = pd.concat(dfs_csv, keys=os.listdir(txts_folder))

start_time = time.time()
lib.write("con", con)
print("writing concatenated: {:f}".format(time.time() - start_time))
start_time = time.time()
conr = lib.read("con").data
print("reading concatenated: {:f}".format(time.time() - start_time))
start_time = time.time()

aggr = []
for txt in os.listdir(txts_folder):
    weight = 2.0
    df = conr.loc[txt].copy()
    df["open_position_profit"] = df["open_position_profit"].multiply(weight).round(2)
    df["gap"] = df["gap"].multiply(weight).round(2)
    df["position"] = df["position"].multiply(weight).round(2)
    df["underlying_vol"] = df["underlying_vol"].multiply(weight).round(2)
    df = df.assign(symbol="ASDD", category="QQQ")
    aggr.append(df[["date", "open_position_profit"]])

print("preparing aggregation: {:f}".format(time.time() - start_time))
res = pd.concat(aggr).groupby('date', as_index=False).sum(level=[1, 2])
