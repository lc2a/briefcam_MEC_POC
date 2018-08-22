FROM selenium/standalone-chrome-debug
CMD  sudo su
CMD  mkdir /docker_consumer
CMD  cd /docker_consumer
WORKDIR /docker_consumer
COPY requirements.txt requirements.txt
RUN  sudo wget https://bootstrap.pypa.io/get-pip.py
RUN  sudo python3 get-pip.py
RUN  sudo pip3 install -r /docker_consumer/requirements.txt
LABEL author="sriramsridharjob78@gmail.com" \
      version="1.0"
ENV   video_file_path_key=/data
ENV   broker_name_key=broker:9092
ENV   topic_key=video-file-name
ENV   case_url_key=http://10.2.40.160/synopsis/#/review/cases
ENV   browser_loc_key=google-chrome
ENV   case_name_key=MEC-POC
RUN  sudo apt-get update
RUN  sudo apt-get install -y \
     net-tools \
     procps
COPY . .
CMD sudo python3 poll_for_new_filename.py
