FROM sebp/elk
RUN echo 'vm.max_map_count = 262144' >> /etc/sysctl.conf
RUN  mkdir /elk
RUN  cd /elk
WORKDIR /elk
COPY . .
