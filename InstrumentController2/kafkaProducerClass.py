from confluent_kafka import Producer
import sys
import logging

logger = logging.getLogger('kafka_instrumentController')
hdlr  = logging.FileHandler("kafka.log")
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)

class kProducer():
    def __init__(self, kafkaBroker):
        self.p = Producer({'bootstrap.servers': kafkaBroker})

    def acked(self, err, msg):

        if err is not None:
            logger.debug("Failed to deliver message: {0}: {1}".format(msg.value(), err.str()))
        else:
            logger.debug("Message produced: {0}".format(msg.value()))


    def SendEvent(self, event, topic, partitionNumber):     
    
        try:
            self.p.produce(topic, event, partition = partitionNumber)
            self.p.poll(0)

        except KeyboardInterrupt:
            pass

        self.p.flush(30)

        
    def SendEvents(self, events, topic, partitionNumber):     
        
        numSent = 0
        try:
            for event in events:
                if numSent > 100:
                    self.p.poll(0.1)
                    numSent = 0
                else:
                    self.p.poll(0)
                self.p.produce(topic, event, partition = partitionNumber)
                numSent += 1

        except KeyboardInterrupt:
            pass

        self.p.flush(30)