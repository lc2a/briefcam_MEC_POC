FROM selenium/standalone-chrome-debug:3.14.0-arsenic
#RUN  sudo apt-get update
#RUN  sudo apt-get install -y \
#     net-tools \
#     vim \
#     python-xlib \
#     procps
RUN  sudo apt-get update
RUN  sudo apt-get install -y scrot
RUN  sudo apt-get install -y net-tools
RUN  sudo apt-get install -y vim
RUN  sudo apt-get install -y wget
RUN  sudo apt-get install -y procps
RUN  sudo apt-get install -y libltdl7
RUN  sudo apt-get install -y iputils-ping
RUN  sudo wget https://bootstrap.pypa.io/get-pip.py
RUN  sudo apt-get install -y apt-transport-https \
     ca-certificates \
     curl \
     gnupg2 \
     software-properties-common
RUN  sudo wget -O - https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
#RUN  curl -fsSL https://download.docker.com/linux/$(. /etc/os-release; echo "$ID")/gpg > /tmp/dkey; apt-key add /tmp/dkey
RUN  sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
RUN  sudo apt-get update
RUN  sudo apt-get install -y docker-ce
RUN  sudo python3 get-pip.py
RUN sudo apt-get install -y vim
RUN sudo apt-get install -y net-tools
RUN sudo apt-get install -y procps
RUN sudo apt-get install -y iputils-ping
RUN  sudo apt-get install -y python-setuptools python-dev build-essential
RUN  sudo add-apt-repository main
RUN  sudo apt-get update
COPY requirements.txt requirements.txt
RUN  sudo pip3 install -r requirements.txt
#ENV   video_file_path_key=/data
#ENV   broker_name_key=broker:9092
#ENV   topic_key=video-file-name
#ENV   case_url_key=http://10.2.40.160/synopsis/#/review/cases
#ENV   browser_loc_key=google-chrome
#ENV   case_name_key=MEC-POC
#CMD touch ~/.Xauthority
#CMD xauth generate :0 . trusted
#RUN xauth add ${HOST}:0 . $(xxd -l 16 -p /dev/urandom)
RUN sudo chown seluser:seluser -R /home/seluser
RUN sudo usermod -aG docker seluser
RUN sudo usermod -aG sudo seluser
RUN sudo mkdir /parent_directory
ADD  infrastructure_components.tar.gz /parent_directory
WORKDIR /parent_directory/infrastructure_components/data_parser
RUN sudo chown seluser:seluser -R /parent_directory/infrastructure_components
RUN sudo chmod 777 -R /parent_directory/infrastructure_components
RUN cat /opt/bin/entry_point.sh >> /parent_directory/infrastructure_components/data_parser/shell_script.sh
#RUN echo "python3 /machine_learning_workers/poll_for_new_filename/poll_for_new_filename.py &" >> /machine_learning_workers/shell_script.sh
RUN sudo chmod 777 /opt/bin/functions.sh
RUN echo "\npython3 /parent_directory/infrastructure_components/data_parser/unittest/test_data_parser.py &" >> /opt/bin/functions.sh
RUN sudo chmod 777 /parent_directory/infrastructure_components/data_parser/shell_script.sh
#RUN  sudo apt-get install -y python3-distutils
CMD ["sh", "-c", "/parent_directory/infrastructure_components/data_parser/shell_script.sh"]
