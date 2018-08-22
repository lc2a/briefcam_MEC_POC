FROM python:3.7-rc-slim
RUN mkdir /docker_consumer
WORKDIR /docker_consumer
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
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
CMD python poll_for_new_filename.py
