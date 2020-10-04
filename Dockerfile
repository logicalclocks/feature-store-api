FROM ubuntu:20.04

RUN apt-get update && \
    apt-get install -y python3-pip git && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install twine \
                 mkdocs \
                 mkdocs-material \
                 git+https://github.com/moritzmeister/keras-autodoc@split-tags

RUN mkdir -p /.local && chmod -R 777 /.local