FROM python:3.7-slim
RUN  mkdir /rtsp_recorder
RUN  cd /rtsp_recorder
WORKDIR /rtsp_recorder
COPY requirements.txt requirements.txt
RUN  apt-get update
RUN  apt-get install -y scrot
RUN  apt-get install -y net-tools
RUN  apt-get install -y vim
RUN  apt-get install -y wget
RUN  apt-get install -y procps
RUN  apt-get install -y livemedia-utils
RUN  wget https://bootstrap.pypa.io/get-pip.py
RUN  python3 get-pip.py
RUN  pip3 install -r requirements.txt
COPY . .
RUN chmod 777 -R /rtsp_recorder
ENV video_file_path_key=/data
ENV broker_name_key=kafka:9092
ENV topic_key=split-video-file
ENV rtsp_file_name_prefix_key=briefcam
ENV rtsp_duration_of_the_video_key=30
ENV rtsp_capture_application_key=openRTSP
CMD python3 rtsp_recorder.py
