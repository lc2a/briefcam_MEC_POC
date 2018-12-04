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
RUN  sudo curl -fsSL https://download.docker.com/linux/$(. /etc/os-release; echo "$ID")/gpg > /tmp/dkey; apt-key add /tmp/dkey
RUN  sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/$(. /etc/os-release; echo "$ID") \
   $(lsb_release -cs) \
   stable"
RUN  sudo apt-get update
RUN  sudo apt-get install -y docker-ce
RUN  sudo apt-get install -y python3-distutils
RUN  sudo python3 get-pip.py
RUN sudo apt-get install -y vim
RUN sudo apt-get install -y net-tools
RUN sudo apt-get install -y procps
RUN sudo apt-get install -y iputils-ping
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
RUN mkdir /parent_directory
RUN mkdir /parent_directory/tier3
RUN mkdir /parent_directory/tier3/machine_learning_workers
WORKDIR /parent_directory/tier3/machine_learning_workers
COPY requirements.txt requirements.txt
RUN  sudo pip3 install -r requirements.txt
COPY . /parent_directory/tier3/machine_learning_workers
ADD  infrastructure_components.tar.gz /machine_learning_workers
RUN sudo chown seluser:seluser -R /parent_directory/tier3/machine_learning_workers
RUN sudo chmod 777 -R /parent_directory/tier3/machine_learning_workers
#RUN sudo echo -e "/machine_learning_workers/poll_for_new_filename/poll_for_new_filename.py &" >> /opt/bin/entry_point.sh
#RUN touch /machine_learning_workers/shell_script.sh
RUN cat /opt/bin/entry_point.sh >> /parent_directory/tier3/machine_learning_workers/shell_script.sh
#RUN echo "python3 /machine_learning_workers/poll_for_new_filename/poll_for_new_filename.py &" >> /machine_learning_workers/shell_script.sh
RUN sudo chmod 777 /opt/bin/functions.sh
RUN echo "\npython3 /parent_directory/tier3/machine_learning_workers/machine_learning_worker.py &" >> /opt/bin/functions.sh
RUN sudo chmod 777 /parent_directory/tier3/machine_learning_workers/shell_script.sh
CMD ["sh", "-c", "/parent_directory/tier3/machine_learning_workers/shell_script.sh"]
