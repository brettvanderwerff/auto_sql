import pandas as pd

def mp_read_csv(nrows, file, sep, skiprows, header, names):
    return pd.read_csv(nrows=nrows, filepath_or_buffer=file, sep=sep, skiprows=skiprows, header=header, names=names)