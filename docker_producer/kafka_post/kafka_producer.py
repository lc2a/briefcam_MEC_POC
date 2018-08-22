from confluent_kafka import Producer
import os

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

broker_name = os.getenv("broker_name_key", default=None)
print("broker_name={}".format(broker_name))
topic = os.getenv("topic_key", default=None)
print("topic={}".format(topic))

def post_filename_to_a_kafka_topic(filename):
    p = Producer({'bootstrap.servers': broker_name})

    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    p.produce(topic, filename.encode('utf-8'), callback=delivery_report)

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    p.flush()
