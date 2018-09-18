import pandas as pd
import os
from psutil import virtual_memory
import sqlite3
import multiprocessing
pd.set_option('display.max_columns', 1)

def get_mem(buffer=.75):
    print("Calculating available RAM")
    return virtual_memory().free * buffer

def get_file_size(file):
    print("Calculating file size")
    return os.path.getsize(file)

def count_file_lines(file):
    print("Counting file rows")
    with open(file) as read_obj:
        line_count = 0
        for line in read_obj:
            line_count += 1
    print(str(line_count) + " Lines found.")
    return line_count

def get_chunk_count(file):
    core_count = multiprocessing.cpu_count()
    file_size = get_file_size(file)
    memory = get_mem()
    if file_size < memory:
        return core_count
    else:
        return (file_size/ memory) * core_count

def write_sql(df):
    con = sqlite3.connect('test.db')
    df.to_sql("df", con, if_exists='append', index=False) # might save header for each df
    con.close()


def read_csv(file, chunk_count, line_count):
    core_count = multiprocessing.cpu_count()
    nrows = int(line_count / chunk_count)
    skiprows = 0
    for core in range(core_count):
        if core == 0:
            df = pd.read_csv(file, sep=",", nrows=nrows)
            skiprows += nrows
            names = df.columns.values
        else:
            df = pd.read_csv(file, sep=",", skiprows=skiprows, nrows=nrows, header=None, names=names) #4329313 total lines nrow 697492
            skiprows += nrows

        write_sql(df)


if __name__ == "__main__":
    chunk_count = get_chunk_count(file="surveys.csv")
    line_count = count_file_lines(file="surveys.csv")
    read_csv(file="surveys.csv", chunk_count=chunk_count, line_count=line_count)
