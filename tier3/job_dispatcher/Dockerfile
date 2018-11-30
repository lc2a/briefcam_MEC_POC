FROM python:3.7-slim
RUN mkdir /parent_directory
RUN mkdir /parent_directory/tier3
RUN mkdir /parent_directory/tier3/job_dispatcher
WORKDIR /parent_directory/tier3/job_dispatcher
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
COPY . /parent_directory/tier3/job_dispatcher
ADD  infrastructure_components.tar.gz /parent_directory
VOLUME /var/run/docker.sock /var/run/docker.sock
VOLUME /etc/timezone /etc/timezone:ro
VOLUME /etc/localtime /etc/localtime:ro
CMD python3 /parent_directory/tier3/job_dispatcher/job_dispatcher.py
