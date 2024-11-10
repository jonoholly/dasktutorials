FROM ubuntu:24.04

RUN apt-get update -y
RUN apt-get upgrade -y
RUN apt-get install curl python3.12 python3-pip -y 

COPY requirements.txt / 
# install requirements. break system packages required for later ubunntu versions if we want to avoid making a veenv.
RUN python3.12 -m pip install -r requirements.txt --no-cache-dir --break-system-packages

WORKDIR /app