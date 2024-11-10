# dasktutorials
trying out some dask tutorials

# Setup

## Get data
curl https://snap.stanford.edu/data/finefoods.txt.gz -o data/finefoods.txt.gz
gunzip data/finefoods.txt.gz

# setup - cnonect to ipython console
```
docker buildx build .
```
# use official images to set up workers

https://docs.dask.org/en/latest/deploying-docker.html
```
cd tests
docker-compose up
```