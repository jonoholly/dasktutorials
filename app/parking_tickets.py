import dask.dataframe as dd
from matplotlib import pyplot as plt
from dask.diagnostics import ProgressBar 

dask_dtypes = {'House Number': 'object',
    'Time First Observed': 'object'
}
df = dd.read_csv('/data/nyc-parking-tickets/*2017.csv', dtype=dask_dtypes)
# lazy
missing_values = df.isnull().sum()
missing_count = ((missing_values / df.index.size) * 100)
# execute
with ProgressBar():
    missing_count_pct = missing_count.compute()
# see which ones missing data
missing_count_pct[missing_count_pct > 60]
# apply filter
columns_to_drop = missing_count_pct[missing_count_pct > 60].index
with ProgressBar():
    df_dropped = df.drop(columns_to_drop, axis=1).persist() 

