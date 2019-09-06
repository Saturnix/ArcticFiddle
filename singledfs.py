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

start_time = time.time()
for i in range(len(dfs_csv)):
    lib.write(str(i), dfs_csv[i])
print("writing singledfs: {:f}".format(time.time() - start_time))
start_time = time.time()
dfs_remote = []
for i in range(len(dfs_csv)):
    dfs_remote.append(lib.read(str(i)).data)
print("reading singledfs: {:f}".format(time.time() - start_time))
start_time = time.time()

aggr = []
for df in dfs_remote:
    weight = 2.0
    df["open_position_profit"] = df["open_position_profit"].multiply(weight).round(2)
    df["gap"] = df["gap"].multiply(weight).round(2)
    df["position"] = df["position"].multiply(weight).round(2)
    df["underlying_vol"] = df["underlying_vol"].multiply(weight).round(2)
    df = df.assign(symbol="ASDD", category="QQQ")
    aggr.append(df[["date", "open_position_profit"]])

print("preparing aggregation: {:f}".format(time.time() - start_time))
res = pd.concat(aggr).groupby('date', as_index=False).sum(level=[1, 2])
