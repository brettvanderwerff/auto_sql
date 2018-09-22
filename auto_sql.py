import pandas as pd
import os
from psutil import virtual_memory
import sqlite3
from multiprocessing import  cpu_count, Process
import math
pd.set_option('display.max_columns', 1)


class AutoSql():
    def __init__(self, file, db_name, sep, buffer=.75):
        self.skiprows = 1
        self.file = file
        self.db_name = db_name
        self.sep = sep
        self.buffer = buffer
        self.names = pd.read_csv(self.file, sep=self.sep, nrows=1).columns

    def get_mem(self):
        print("Calculating available RAM...")
        memory = virtual_memory().free * self.buffer
        str_mem = str(round(memory / 10 ** 9, 2))
        print(str_mem + " GB available RAM detected\n")
        return memory

    def get_file_size(self):
        print("Calculating file size...")
        file_size = os.path.getsize(self.file)
        str_file_size = str(round(file_size/10**9, 2))
        print(str_file_size + " GB file detected\n")
        return os.path.getsize(self.file)

    def count_file_lines(self):
        print("Counting file rows...")
        with open(self.file) as read_obj:
            read_obj.readline() #Read a line to skip the header
            line_count = 0
            for line in read_obj:
                line_count += 1
                if line_count%100000 == 0:
                    print(str(line_count) + ' lines read')
        print('\n' + str(line_count) + " lines found")
        return line_count

    def get_chunk_count(self):
        core_count = cpu_count()
        file_size = self.get_file_size()
        memory = self.get_mem() #uncomment for production and delete below line
        #memory = 1011723776
        if file_size < memory:
            return core_count
        else:
            return math.ceil((file_size / memory) * core_count)

    def write_sql(self, df):
        print('Writing chunk to disk')
        table_name = os.path.basename(self.file).split(sep='.')[0]
        con = sqlite3.connect('{}.db'.format(self.db_name))
        df.to_sql(table_name , con, if_exists='append', index=False)
        con.close()

    def get_line_list(self, line_count, chunk_count):
        line_list = []
        for i in range(1, chunk_count):
            line_list.append(line_count // chunk_count)
        line_list.append(line_count // chunk_count + line_count % chunk_count)
        return line_list


    def read_csv(self, inner_line_list):
        for counter, line_count in enumerate(inner_line_list):
            print('Reading chunk to memory')
            df = pd.read_csv(self.file, sep=self.sep, skiprows=self.skiprows, nrows=line_count, header=None, names=self.names)
            self.skiprows += line_count
            self.write_sql(df=df)


    def mp_handler(self, line_list):
        core_count = cpu_count()
        mp_list = [line_list[i:i + core_count] for i in range(0, len(line_list), core_count)]
        for inner_line_list in mp_list:
            self.read_csv(inner_line_list=inner_line_list)


    def run(self):
        chunk_count = self.get_chunk_count()
        line_count = self.count_file_lines()
        line_list = self.get_line_list(line_count=line_count, chunk_count=chunk_count)
        self.mp_handler(line_list=line_list)


if __name__ == "__main__":
    my_object = AutoSql(file='surveys.csv', db_name='database', sep=',')
    my_object.run()

