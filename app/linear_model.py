# script meant to be run in ipython
import dask.bag as bag
import os
from dask.diagnostics import ProgressBar

raw_data = bag.read_text("/data/finefoods.txt")

def get_next_part(file, start_index, span_index=0, blocksize=1024):
    file.seek(start_index)
    buffer = file.read(blocksize + span_index).decode('cp1252')
    delimiter_position = buffer.find('\n\n')
    if delimiter_position == -1:
        return get_next_part(file, start_index, span_index + blocksize)
    else:
        file.seek(start_index)
        return stat_index, delimiter_position