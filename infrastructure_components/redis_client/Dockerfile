FROM ubuntu
RUN  apt-get update
RUN  apt-get install -y scrot
RUN  apt-get install -y net-tools
RUN  apt-get install -y vim
RUN  apt-get install -y wget
RUN  apt-get install -y procps
RUN  apt-get install -y libltdl7
RUN  apt-get install -y iputils-ping
RUN  wget https://bootstrap.pypa.io/get-pip.py
RUN  apt-get install -y apt-transport-https \
     ca-certificates \
     curl \
     gnupg2 \
     software-properties-common
RUN  curl -fsSL https://download.docker.com/linux/$(. /etc/os-release; echo "$ID")/gpg > /tmp/dkey; apt-key add /tmp/dkey
RUN  add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/$(. /etc/os-release; echo "$ID") \
   $(lsb_release -cs) \
   stable"
RUN  apt-get update
RUN  apt-get install -y docker-ce
RUN apt-get install -y python3-distutils
RUN  python3 get-pip.py
RUN apt-get install -y vim
RUN apt-get install -y net-tools
RUN apt-get install -y procps
RUN apt-get install -y iputils-ping
COPY requirements.txt requirements.txt
RUN  pip3 install -r requirements.txt
RUN mkdir /parent_directory
WORKDIR /parent_directory/infrastructure_components/redis_client
ADD  infrastructure_components.tar.gz /parent_directory
