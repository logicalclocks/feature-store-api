FROM ubuntu:20.04

RUN apt-get update && \
    apt-get install -y python3-pip && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install twine 