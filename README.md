# briefcam_MEC_POC
This is a Mobile Edge Compute platform Proof Of Concept (POC).

The POC is to demonstrate live object detection (Person, vehicle) from surveillance traffic camera video feed on the Edge Compute network and thereby proving the Edge compute network capabilities such as Low Latency Compute power and 4G LTE / 5G  Bandwidth savings for the Mobile Operator.

There are 3 Tiers to this Architecture.

TIER 1:

An Administrator logins to CouchBD via Fauxton front end web interface.
After logging in, the administrator creates a database named “briefcam” and creates a document in this talisting the traffic camera details such as (4G LTE IP address or Domain name).

Note 1: The Adminstrator is allowed to create/update/delete a camera information.
Note 2: Dockers (ssriram1978/front_end:latest, 3apaxicom/fauxton, wurstmeister/kafka:latest, couchdb, wurstmeister/zookeeper) are used in this TIER 1.
Note 3:  In front_end directory, run the docker-compose command 
docker-compose -d up
docker-compose down

TIER 2:

The traffic camera information is passed on to the RTSP (Real Time Streaming Protocol) service infrastructure which connects to the RTSP server hosted on the Traffic camera via the provided credentials (username, password, IP address,...) and start capturing the video feed.
This video feed is split into 30 second video clips and passed down to the Machine learning algorithms that identifies objects in the video feeds.

Note: Dockers (ssriram1978/rtsp_recorder:latest, wurstmeister/kafka:latest, couchdb, wurstmeister/zookeeper) are used in this TIER 2.

Note 3:  In rtsp_recorder directory, run the docker-compose command 
docker-compose -d up
docker-compose down

TIER 3:

The video feeds are fed into the machine learning algorithm compute network by a swarm of workers and the results can be queried and fetched via the graphical user interface portal via the hosted web service. ( provided by a Vendor -  https://www.briefcam.com/) 

Any anomalies or triggers set based upon a certain pattern (RED color vehicle, person wearing glasses, person with black hair) can be set/queried/fetched via the webportal.

Note 1: Dockers (ssriram1978/job_dispatcher:latest, ssriram1978/machine_learning_workers:latest, wurstmeister/kafka:latest, portainer/portainer, redis:latest, wurstmeister/zookeeper) are used in this TIER 3.
Note 2: In machine_learning_workers directory, run the docker stack command. 
“docker stack deploy -c docker-compose.yml machine_learning_workers”
“docker stack rm machine_learning_workers”

To run all 3 Tiers together, 
In the main briefcam directory, run 
“docker stack deploy -c docker-compose.yml briefcam”
“docker stack rm briefcam”
