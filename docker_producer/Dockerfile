FROM python:3.7-slim
RUN mkdir /docker_producer
WORKDIR /docker_producer
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
LABEL author="sriramsridharjob78@gmail.com" \
      version="1.0"
ENV   video_file_path_key=/data
ENV   broker_name_key=broker:9092
ENV   topic_key=video-file-name
RUN apt-get update
RUN apt-get install -y \
    net-tools \
    procps

COPY . .
CMD python3 directory_watch.py
