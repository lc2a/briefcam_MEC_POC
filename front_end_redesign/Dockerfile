FROM python:3.7-slim
RUN  mkdir /front_end_redesign
RUN  cd /front_end_redesign
WORKDIR /front_end_redesign
COPY requirements.txt requirements.txt
RUN  apt-get update
RUN  apt-get install -y scrot
RUN  apt-get install -y net-tools
RUN  apt-get install -y vim
RUN  apt-get install -y wget
RUN  apt-get install -y procps
RUN  apt-get install -y libltdl7
RUN  wget https://bootstrap.pypa.io/get-pip.py
RUN  python3 get-pip.py
RUN  pip3 install -r requirements.txt
COPY . .
RUN chmod 777 -R /front_end_redesign
CMD python3 front_end/front_end.py
