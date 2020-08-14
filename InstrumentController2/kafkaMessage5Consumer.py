import json
from kafka import KafkaConsumer, TopicPartition


class KafkaConsumerM5():
    def __init__(self, kafkaBroker, topic):
        self.kafkaBroker = kafkaBroker

        self.consumer = KafkaConsumer(
            bootstrap_servers = self.kafkaBroker, 
            auto_offset_reset = 'latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        tp = TopicPartition(topic, 0)
        self.consumer.assign([tp])

    def WaitforSequenceComplete(self, numExecutors):  
        data = []
        numMsgs = 0
        
        try:
            while True:
                message = next(self.consumer)
                data.append(message.value)
                numMsgs = numMsgs+1
                    
                #print(message.value)
                if numMsgs == numExecutors:
                    print('Num Messages Received:{}'.format(numMsgs))
                    return data
        except KeyboardInterrupt:
            print("Closing Consumer")
            self.consumer.close()
            pass
                   

if(__name__ == '__main__'):

    kafkaBroker = "simfarm14.ni.corp.natinst.com:9092"
    kc = KafkaConsumerM5(kafkaBroker, "sequenceComplete")
    data = kc.WaitforSequenceComplete(1)
    print(data)
    #f = open("temp.json", "w")
    #f.write(json.dumps(data))
    #f.close()