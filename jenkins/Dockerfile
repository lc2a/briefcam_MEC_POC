FROM jenkins/jenkins:lts
ENV JENKINS_SLAVE_AGENT_PORT 50001
# if we want to install via apt
USER root
RUN apt-get update && apt-get install -y maven
RUN apt-get update \
  && apt-get install -y python3-pip python3-dev \
  && cd /usr/local/bin \
  && ln -s /usr/bin/python3 python \
  && pip3 install --upgrade pip
COPY . .
RUN  pip3 install -r requirements.txt
# drop back to the regular jenkins user - good practice
USER jenkins