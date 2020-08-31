from kafka import KafkaProducer
import json
class kProducer():
    def __init__(self, kafkaBroker):
        self.p = KafkaProducer(
                                bootstrap_servers = kafkaBroker,
                                value_serializer=lambda x:json.dumps(x).encode('utf-8')
                            )
    
    def SendStartEvent(self, numExecutors, topic):     

        try:
            sequenceStart = {
                    "Message": "Start",
                    "NumExecutors": 0,
                    "StepPartition": -1,
            }
            sequenceStart['NumExecutors'] = numExecutors
            self.p.send(topic = topic, value = sequenceStart, partition = 0)
                
        except KeyboardInterrupt:
            pass

        self.p.flush(30)
    
    def SendStopEvent(self, topic):     

        try:
            sequenceStop = {
                    "Message": "Stop",
                    "NumExecutors": 0,
                    "StepPartition": -1,
            }
            self.p.send(topic = topic, value = sequenceStop, partition = 0)
                
        except KeyboardInterrupt:
            pass

        self.p.flush(30)