FROM ubuntu:20.04

RUN apt-get update && \
    apt-get install -y python3-pip git && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install twine \
                 mkdocs==1.1.2 \
                 mkdocs-material==6.2.2 \
                 mike==0.5.5 \
                 git+https://github.com/moritzmeister/keras-autodoc@split-tags-properties

RUN mkdir -p /.local && chmod -R 777 /.local
