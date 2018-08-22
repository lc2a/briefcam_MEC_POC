# briefcam_MEC_POC
This is Mobile Edge compute platform capbility POC demo.
This python script demonstrates the usage of pyautogui to automate windows keyboard and mouse clicks and watchdog events to demonstrate monitoring of new files added/deleted in a specified watch directory.

To run:
docker run -it -v C:\BriefCam\ServerData\VideoData\VideoFiles\fromMobile:/data ssriram1978/docker_producer /bin/sh
docker run -it -v C:\BriefCam\ServerData\VideoData\VideoFiles\fromMobile:/data ssriram1978/docker_consumer /bin/sh
