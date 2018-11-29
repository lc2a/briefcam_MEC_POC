FROM python:3.7-slim
RUN mkdir /job_dispatcher
WORKDIR /job_dispatcher/directory_watch
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
LABEL author="sriramsridharjob78@gmail.com" \
      version="1.0"
#ENV   video_file_path_key=/data
#ENV   broker_name_key=broker:9092
#ENV   topic_key=video-file-name
RUN apt-get update
RUN apt-get install -y vim
RUN apt-get install -y net-tools
RUN apt-get install -y procps
RUN apt-get install -y iputils-ping
COPY . .
CMD python3 job_dispatcher/job_dispatcher.py
