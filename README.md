# dasktutorials
trying out some dask tutorials

# Setup

## Get data
```bash
curl https://snap.stanford.edu/data/finefoods.txt.gz -o data/finefoods.txt.gz
gunzip data/finefoods.txt.gz
```

## get repo (ch11 files helpful)
```bash
git clone https://github.com/jcdaniel91/data-science-python-dask.git
```

https://www.kaggle.com/datasets/new-york-city/nyc-parking-tickets?resource=download

# setup - cnonect to ipython console
```bash
docker buildx build .
```
# use official images to set up workers

https://docs.dask.org/en/latest/deploying-docker.html
```bash
cd tests
docker-compose up
```