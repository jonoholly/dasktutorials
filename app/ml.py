# uses preprocessed result from bags.py
import dask.bag as bag
import os
from dask.delayed import delayed
from dask.diagnostics import ProgressBar
import numpy as np
from dask import array as darr
from dask_ml.linear_model import LogisticRegression
from dask_ml.model_selection import train_test_split
from sklearn.naive_bayes import BernoulliNB
from dask_ml.wrappers import Incremental
from dask_ml.model_selection import GridSearchCV
import pandas as pd

# load preprocessed result from bags.py
feature_array = darr.from_zarr("/data/sentiment_feature_array.zarr")
target_array = darr.from_zarr("/data/sentiment_target_array.zarr")

CHUNK_SIZE = (
    100000  # 5000 in tutorial. noted that CPU and mem usage way up with higher chunk.
)
X = feature_array.rechunk(CHUNK_SIZE)  # bigger splits
y = target_array.rechunk(CHUNK_SIZE).flatten()
# split and train data!
X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=42)

lr = LogisticRegression()

with ProgressBar():
    lr.fit(X_train, y_train)

lr.score(X_test, y_test).compute()

nb = BernoulliNB()
parallel_nb = Incremental(nb)
with ProgressBar():
    parallel_nb.fit(X_train, y_train, classes=[0, 1])
parallel_nb.score(X_test, y_test)

parameters = {"penalty": ["l1", "l2"], "C": [0.5, 1, 2]}

lr = LogisticRegression()
tuned_lr = GridSearchCV(lr, parameters)

with ProgressBar():
    tuned_lr.fit(X_train, y_train)

pd.DataFrame(tuned_lr.cv_results_)
