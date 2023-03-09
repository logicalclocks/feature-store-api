FROM hopsworks/locust:hsfs-latest

USER root

RUN apt-get update -y && apt-get -y install gcc python3-dev librdkafka-dev git

USER locust
WORKDIR /home/locust

COPY requirements.txt .
RUN pip3 install -r requirements.txt
