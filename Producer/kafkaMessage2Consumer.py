import json
from kafka import KafkaConsumer, TopicPartition


class KafkaConsumerM2():
    def __init__(self, kafkaBroker, topic):
        self.kafkaBroker = kafkaBroker

        self.consumer = KafkaConsumer(
            bootstrap_servers = self.kafkaBroker, 
            auto_offset_reset = 'latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        tp = TopicPartition(topic, 0)
        self.consumer.assign([tp])

    def WaitforSequenceStart(self):  
        try:
            while True:
                message = next(self.consumer)
                return message.value
               
        except KeyboardInterrupt:
            print("Closing Message 2 Consumer")
            self.consumer.close()
            pass
                   

if(__name__ == '__main__'):

    kafkaBroker = "simfarm14.ni.corp.natinst.com:9092"
    kc = KafkaConsumerM2(kafkaBroker, "sequenceIter")
    data = kc.WaitforSequenceStart()
    print(data)
