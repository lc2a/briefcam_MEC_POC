import logging
from datetime import datetime
import os
import sys

hostname = os.popen("cat /etc/hostname").read()
cont_id = os.popen("cat /proc/self/cgroup | head -n 1 | cut -d '/' -f3").read()

print("__name__={}".format(__name__))
module_main = sys.modules['__main__']
print("sys.modules['__main__']={}".format(module_main))
module_file = module_main.__file__
print("sys.modules['__main__'].__file__={}".format(module_file))
module_name = module_file.rstrip('.py')
print("Using module_name={}.log for logging.".format(module_name))

logging.basicConfig(format='(%(threadName)-2s:'
                           '%(levelname)s:'
                           '%(asctime)s:'
                           '%(lineno)d:'
                           '%(filename)s:'
                           '%(funcName)s:'
                           '%(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    filename='{}.log'.format(module_name),
                    level=logging.DEBUG)


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


