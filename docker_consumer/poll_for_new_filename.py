from confluent_kafka import Consumer, KafkaError
import os
from upload_video.upload_video_to_briefcam import process_new_file

if __name__=='__main__':
    broker_name = os.getenv("broker_name_key", default=None)
    print("broker_name={}".format(broker_name))
    topic = os.getenv("topic_key", default=None)
    print("topic={}".format(topic))

    c = Consumer({
        'bootstrap.servers': broker_name,
        'group.id': 'mygroup',
        'default.topic.config': {
        'auto.offset.reset': 'smallest'
        }
    })

    c.subscribe([topic])

    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
        else:
            print(msg.error())
            break
        filename=msg.value().decode('utf-8')
        print('Received message: {}'.format(filename))
        process_new_file(filename)
    c.close()
