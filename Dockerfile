FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y python3-pip git && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install twine build virtualenv \
    mkdocs==1.5.3 \
    mkdocs-material==9.5.17 \
    mike==2.0.0 \
    git+https://github.com/logicalclocks/keras-autodoc

RUN mkdir -p /.local && chmod -R 777 /.local
