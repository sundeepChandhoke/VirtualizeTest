import json
from kafka import KafkaConsumer, TopicPartition


class KafkaConsumerM3():
    def __init__(self, kafkaBroker, topic, pt, replay):
        self.kafkaBroker = kafkaBroker
        self.topic = topic
        self.partition = pt
        
        if replay == True:
            offsetReset = 'earliest'
        else:
            offsetReset = 'latest'

        self.consumer = KafkaConsumer(
            bootstrap_servers = self.kafkaBroker, 
            auto_offset_reset = offsetReset,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        tp = TopicPartition(self.topic, self.partition)
        self.consumer.assign([tp])

    def GetData(self):  
        import sys
        data = []
        numMsgs = 0
        
        try:
            while True:
                message = next(self.consumer)
                if message.value['Status'] == 'Start':
                    print(message.offset)
                data.append(message.value)
                numMsgs = numMsgs+1
                    
                #print(message.value)
                if message.value['Status'] == 'End':
                    print('Num Messages Received:{}'.format(numMsgs))
                    return data
        except KeyboardInterrupt:
            print("Closing Consumer")
            self.consumer.close()
            pass
                   

if(__name__ == '__main__'):

    kafkaBroker = "simfarm14.ni.corp.natinst.com:9092"
    kc = KafkaConsumerM3(kafkaBroker, "qamData", 0)
    data = kc.GetData()
    f = open("temp.json", "w")
    f.write(json.dumps(data))
    f.close()