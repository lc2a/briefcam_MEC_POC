import time
time.sleep(60)

from confluent_kafka import Consumer, KafkaError
import os
import logging
import sys

logging.basicConfig(format='(%(threadName)-2s:'
                                       '%(levelname)s:'
                                       '%(asctime)s:'
                                       '%(lineno)d:'
                                       '%(filename)s:'
                                       '%(funcName)s:'
                                       '%(message)s',
                                datefmt='%m/%d/%Y %I:%M:%S %p',
                                filename='poll_for_new_filename.log',
                                level=logging.DEBUG)

if __name__=='__main__':
    broker_name = os.getenv("broker_name_key", default=None)
    logging.debug("broker_name={}".format(broker_name))
    topic = os.getenv("topic_key", default=None)
    logging.debug("topic={}".format(topic))
  
    c=None
    while c==None:
        c = Consumer({
            'bootstrap.servers': broker_name,
            'group.id': 'mygroup',
            'default.topic.config': {
            'auto.offset.reset': 'smallest'
            }
        })
    logging.debug('Successfully attached to bootstrap server={},'.format(broker_name))
    c.subscribe([topic])
    logging.debug('Successfully subscribed to topic={},'.format(topic))
    connected=False
    while connected==False:
        try:
            from upload_video.upload_video_to_briefcam import process_new_file
        except:
            logging.debug(sys.exc_info()[0])
            print(sys.exc_info()[0])
        else:
             logging.debug("successfully connected to xhost display")
             print("successfully connected to xhost display")
             connected=True        

    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
        else:
            logging.debug(msg.error())
            break
        filename=msg.value().decode('utf-8')
        logging.debug('Received message: {}'.format(filename))
        process_new_file(filename)
    c.close()
