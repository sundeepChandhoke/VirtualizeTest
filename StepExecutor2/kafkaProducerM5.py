from kafka import KafkaProducer
import json
class kProducer():
    def __init__(self, kafkaBroker):
        self.p = KafkaProducer(
                                bootstrap_servers = kafkaBroker,
                                value_serializer=lambda x:json.dumps(x).encode('utf-8')
                            )
    
    def SendEvents(self, events, topic):     

        try:
            for event in events:
                self.p.send(topic = topic, value = event, partition = 0)
                
        except KeyboardInterrupt:
            pass

        self.p.flush(30)