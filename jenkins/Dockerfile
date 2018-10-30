FROM jenkins/jenkins:lts
ENV JENKINS_SLAVE_AGENT_PORT 50001
# if we want to install via apt
USER root
RUN apt-get update && apt-get install -y maven
# drop back to the regular jenkins user - good practice
USER jenkins
