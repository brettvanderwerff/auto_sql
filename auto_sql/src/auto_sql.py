from multiprocessing import  cpu_count, Pool
import math
import os
import pandas as pd
from psutil import virtual_memory
import sqlite3

class AutoSql():
    def __init__(self, file, out_dir, sep=',', buffer=.3, db_name=None, encoding=None):
        '''
        Instantiates the AutoSql object.

        :param file: path to the csv to be processed.
        :param db_name: name of the sqlite database that will be created.
        :param out_dir: full path to where database will be written.
        :param sep: the delimiter of the csv
        :param buffer: int from 0-1, allows user to toggle amount of memory AutoSql will try to fill at a given time.
        If memory error, reduce this number.
        :param encoding: Encoding to use when reading csv.

        '''
        self.skiprows = 1
        self.file = file
        self.out_dir = out_dir
        self.sep = sep
        self.buffer = buffer
        self.names = pd.read_csv(self.file, sep=self.sep, nrows=1).columns
        self.encoding = encoding

        if db_name == None:
            self.db_name = os.path.basename(self.file).split(sep='.')[0]
        else:
            self.db_name = db_name

    def get_mem(self):
        '''
        Determines the amount of free RAM the user has for loading chunks of the csv file into .

        :return: memory: the amount of available memory in bytes.
        '''
        print("Calculating available RAM...")
        memory = virtual_memory().free * self.buffer
        str_mem = str(round(memory / 10 ** 9, 2))
        print(str_mem + " GB available RAM detected\n")
        return memory

    def get_file_size(self):
        '''
        Calculates the size of a file.

        :return: file_size: the size of the file in bytes.
        '''
        print("Calculating file size...")
        file_size = os.path.getsize(self.file)
        str_file_size = str(round(file_size/10**9, 2))
        print(str_file_size + " GB file detected\n")
        file_size = os.path.getsize(self.file)
        return file_size

    def count_file_lines(self):
        '''
        Counts the number of lines in a file.

        :return: line_count: the number of lines in the file.
        '''
        print("Counting file rows...")
        with open(self.file) as read_obj:
            read_obj.readline() #Read a line to skip the header
            line_count = 0
            for line in read_obj:
                line_count += 1
                if line_count%1000000 == 0:
                    print(str(line_count) + ' lines read')
        print('\n' + str(line_count) + " lines found")
        return line_count

    def get_chunk_count(self):
        '''
        Determines the number of chunks a file need to be divided by to allow each chunk to fit in memory.

        :return: chunk_count: the number of chunks the file needs to be divided by.
        '''
        core_count = cpu_count()
        file_size = self.get_file_size()
        memory = self.get_mem()
        if file_size < memory:
            chunck_count = core_count
            print('Chunk count: ' + str(chunck_count))
            return chunck_count
        else:
            chunck_count =  math.ceil((file_size / memory) * core_count)
            print('Chunk count: ' + str(chunck_count))
            return chunck_count

    def write_sql(self, dfs):
        '''
        Writes a list of pandas DataFrames to a sqlite database.

        :param dfs: A list of pandas DataFrames.
        '''
        print('Flushing to disk')
        db_path = os.path.join(self.out_dir, os.path.basename(self.db_name + '.db'))
        table_name = os.path.basename(self.file).split(sep='.')[0]
        con = sqlite3.connect(db_path)
        for df in dfs:
            df.to_sql(table_name , con, if_exists='append', index=False)
        con.close()

    def get_line_list(self, line_count, chunk_count):
        '''
        Takes the total number of lines counted in the csv and divides the count into a list of lines that can be
        read into memory without causing a memory error.

        :param line_count: total number of lines in csv.
        :param chunk_count: The number of chunks line_count needs to be divided by to fit in memory.
        :return: line_list: a list of lines of a file that can be read into memory at one time w/o error.
        '''
        line_list = []
        for i in range(1, chunk_count):
            line_list.append(line_count // chunk_count)
        line_list.append(line_count // chunk_count + line_count % chunk_count)
        return line_list

    def mp_read_csv(self, nrows, file, sep, skiprows, header, names, encoding):
        '''
        Worker function for multiprocessing pool started in the read_csv method. parameters are analogous to pandas
        DataFrame read_csv method.

        :param nrows: number of rows to read.
        :param file: path to csv file.
        :param sep: delimiter of csv.
        :param skiprows: number of rows to skip before reading csv.
        :param header: Indicates if header present in csv.
        :param names: names within header column.
        :return:
        '''
        return pd.read_csv(nrows=nrows,
                           filepath_or_buffer=file,
                           sep=sep, skiprows=skiprows,
                           header=header,
                           names=names,
                           encoding=encoding)


    def read_csv(self, inner_line_list):
        '''
        Reads several chunk of a csv into memory based using multiprocessing. The number of chunks read at a time.
        correspond to the number of processor cores.

        :param inner_line_list: A list of lines that are used to subset the csv into chunks for each core.
        '''
        print('Reading chunks to memory')
        outer_arg_list = []
        for counter, line_count in enumerate(inner_line_list):
            inner_arg_list = []
            inner_arg_list.extend((line_count, self.file, self.sep, self.skiprows, None, self.names, self.encoding))
            outer_arg_list.append(inner_arg_list)
            self.skiprows += line_count
        pool = Pool(len(inner_line_list))
        dfs = pool.starmap(self.mp_read_csv, outer_arg_list)
        pool.close()
        pool.join()
        self.write_sql(dfs=dfs)


    def mp_handler(self, line_list):
        '''
        Subsets line_list according to how many cores the processor has, passes this subset to read_csv

        :param line_list: A list of lines that correspond to the number of rows a csv needs to be divided by for
        reading chunks of the csv that will fit in memory.
        '''
        core_count = cpu_count()
        mp_list = [line_list[i:i + core_count] for i in range(0, len(line_list), core_count)]
        for inner_line_list in mp_list:
            self.read_csv(inner_line_list=inner_line_list)

    def run(self):
        '''
        Calls all methods to run the script.
        '''
        chunk_count = self.get_chunk_count()
        line_count = self.count_file_lines()
        line_list = self.get_line_list(line_count=line_count, chunk_count=chunk_count)
        self.mp_handler(line_list=line_list)
        print('Complete')


if __name__ == "__main__":
    my_object = AutoSql(file='file.csv',
                        sep=',',
                        out_dir=".")
    my_object.run()

