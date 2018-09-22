import pandas as pd
import os
from psutil import virtual_memory
import sqlite3
import multiprocessing
import math
import numpy as np
pd.set_option('display.max_columns', 1)

def get_mem(buffer=.75):
    print("Calculating available RAM...")
    mem = virtual_memory().free * buffer
    str_mem = str(round(mem / 10 ** 9, 2))
    print(str_mem + " GB available RAM detected\n")
    return mem

def get_file_size(file):
    print("Calculating file size...")
    file_size = os.path.getsize(file)
    str_file_size = str(round(file_size/10**9, 2))
    print(str_file_size + " GB file detected\n")
    return os.path.getsize(file)

def count_file_lines(file):
    print("Counting file rows...")
    with open(file) as read_obj:
        read_obj.readline() #Read a line to skip the header
        line_count = 0
        for line in read_obj:
            line_count += 1
            if line_count%100000 == 0:
                print(str(line_count) + ' lines read')
    print('\n' + str(line_count) + " lines found")
    return line_count

def get_chunk_count(file):
    core_count = multiprocessing.cpu_count()
    file_size = get_file_size(file)
    memory = get_mem()
    if file_size < memory:
        return core_count
    else:
        return math.ceil((file_size / memory) * core_count)

def write_sql(file, df, db_name):
    print('Writing chunk to disk')
    table_name = os.path.basename(file).split(sep='.')[0]
    con = sqlite3.connect('{}.db'.format(db_name))
    df.to_sql(table_name , con, if_exists='append', index=False)
    con.close()

def get_line_list(line_count, chunk_count):
    line_list = []
    for i in range(1, chunk_count):
        line_list.append(line_count // chunk_count)
    line_list.append(line_count // chunk_count + line_count % chunk_count)
    return line_list


def read_csv(file, line_list, db_name, sep):
    mp_list = np.array_split(line_list, 4)
    print(mp_list)
    skiprows = 0
    for counter, line_count in enumerate(line_list):
        print(line_list)
        if counter == 0:
            df = pd.read_csv(file, sep=sep, nrows=line_count)
            skiprows += line_count
            names = df.columns.values
        else:
            df = pd.read_csv(file, sep=sep, skiprows=skiprows, nrows=line_count, header=None, names=names)
            skiprows += line_count

        write_sql(file=file, df=df, db_name=db_name)

def run(file, db_name, sep):
    chunk_count = get_chunk_count(file=file)
    line_count = count_file_lines(file=file)
    line_list = get_line_list(line_count=line_count, chunk_count=chunk_count)
    read_csv(file=file, line_list=line_list, db_name=db_name, sep=sep)


if __name__ == "__main__":
    run(file='test.txt', db_name='database', sep='\t')
