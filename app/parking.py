import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from functools import reduce
import numpy as np
import pandas as pd

DATADIR = "/data/nyc-parking-tickets/"

# use real schema from kaggle

dtypes = {
 'Date First Observed': str,
 'Days Parking In Effect    ': str,
 'Double Parking Violation': str,
 'Feet From Curb': np.float32,
 'From Hours In Effect': str,
 'House Number': str,
 'Hydrant Violation': str,
 'Intersecting Street': str,
 'Issue Date': str,
 'Issuer Code': np.float32,
 'Issuer Command': str,
 'Issuer Precinct': np.float32,
 'Issuer Squad': str,
 'Issuing Agency': str,
 'Law Section': np.float32,
 'Meter Number': str,
 'No Standing or Stopping Violation': str,
 'Plate ID': str,
 'Plate Type': str,
 'Registration State': str,
 'Street Code1': np.uint32,
 'Street Code2': np.uint32,
 'Street Code3': np.uint32,
 'Street Name': str,
 'Sub Division': str,
 'Summons Number': np.uint32,
 'Time First Observed': str,
 'To Hours In Effect': str,
 'Unregistered Vehicle?': str,
 'Vehicle Body Type': str,
 'Vehicle Color': str,
 'Vehicle Expiration Date': str,
 'Vehicle Make': str,
 'Vehicle Year': np.float32,
 'Violation Code': np.uint16,
 'Violation County': str,
 'Violation Description': str,
 'Violation In Front Of Or Opposite': str,
 'Violation Legal Code': str,
 'Violation Location': str,
 'Violation Post Code': str,
 'Violation Precinct': np.float32,
 'Violation Time': str
}
# load all the files
fy14 = dd.read_csv(DATADIR+'Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv', dtype=dtypes, usecols=dtypes.keys())
fy15 = dd.read_csv(DATADIR +'Parking_Violations_Issued_-_Fiscal_Year_2015.csv',dtype=dtypes, usecols=dtypes.keys())
fy16 = dd.read_csv(DATADIR+'Parking_Violations_Issued_-_Fiscal_Year_2016.csv',dtype=dtypes, usecols=dtypes.keys())
fy17 = dd.read_csv(DATADIR+'Parking_Violations_Issued_-_Fiscal_Year_2017.csv',dtype=dtypes, usecols=dtypes.keys())

fy17

# find common columns - mismatch
columns = [set(fy14.columns),
    set(fy15.columns),
    set(fy16.columns),
    set(fy17.columns)]
common_columns = list(reduce(lambda a, i: a.intersection(i), columns))

# create scema and apply to fy14 - just everything is a string

dtype_tuples = [(x, str) for x in common_columns]
dtypes = dict(dtype_tuples)
fy14 = dd.read_csv(DATADIR+'Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv', dtype=dtypes)

raw_data = dd.read_csv(DATADIR+'*.csv', dtype=dtypes, usecols=common_columns)
cols = ['Plate ID', 'Registration State']
# get head of a few columns
with ProgressBar():
    print(raw_data[cols].head())


# drop some columns
violation_cols = list(filter(lambda columnName: 'Violation' in columnName, raw_data.columns))

with ProgressBar():
    print(raw_data.drop(violation_cols, axis=1).head())

# concat is same as pandas. NOTE - huge advantage here is you can run the operation without compute to make sure it makes sense
fy1617 = dd.concat([fy16,fy17])

with ProgressBar():
    print(fy16['Summons Number'].count().compute())

with ProgressBar():
    print(fy17['Summons Number'].count().compute())

with ProgressBar():
    print(fy1617['Summons Number'].count().compute())