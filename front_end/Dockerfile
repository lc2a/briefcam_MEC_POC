FROM python:3.7-slim
RUN  mkdir /Front_end
RUN  cd /Front_end
WORKDIR /Front_end
COPY requirements.txt requirements.txt
RUN  apt-get update
RUN  apt-get install -y scrot
RUN  apt-get install -y net-tools
RUN  apt-get install -y vim
RUN  apt-get install -y wget
RUN  apt-get install -y procps
RUN  wget https://bootstrap.pypa.io/get-pip.py
RUN  python3 get-pip.py
RUN  pip3 install -r requirements.txt
COPY . .
RUN chmod 777 -R /Front_end
ENV broker_name_key=kafka:9092
ENV topic_key=split-video-file
ENV couchdb_server_key=couchdb:5984
ENV database_name_key=briefcam
CMD python3 front_end.py