import logging
from datetime import datetime
import os

hostname = os.popen("cat /etc/hostname").read()
cont_id = os.popen("cat /proc/self/cgroup | head -n 1 | cut -d '/' -f3").read()

logging.basicConfig(format='(%(threadName)-2s:'
                           '%(levelname)s:'
                           '%(asctime)s:'
                           '%(lineno)d:'
                           '%(filename)s:'
                           '%(funcName)s:'
                           '%(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    filename='rtsp_recorder_orchestrator.log',
                    level=logging.INFO)


def logging_to_console_and_syslog(log, level=logging.INFO):
    if level == logging.DEBUG:
        logging.debug("hostname=" + hostname + " " + "containerID=" + cont_id[:12] + " " + log)
    elif level == logging.INFO:
        logging.info("hostname=" + hostname + " " + "containerID=" + cont_id[:12] + " " + log)
    elif level == logging.WARNING:
        logging.warning("hostname=" + hostname + " " + "containerID=" + cont_id[:12] + " " + log)
    elif level == logging.ERROR:
        logging.error("hostname=" + hostname + " " + "containerID=" + cont_id[:12] + " " + log)
    elif level == logging.CRITICAL:
        logging.critical("hostname=" + hostname + " " + "containerID=" + cont_id[:12] + " " + log)

    #print it to stdout.
    i = datetime.now()
    print(str(i) + " hostname={} containerID={} ".format(hostname, cont_id[:12]) + log)


