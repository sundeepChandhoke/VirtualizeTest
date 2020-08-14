import json
import sys
import kafkaProducerM3 as kafka
import kafkaMessage5Consumer as kafkaConsumer
import threading

message3 = {
    "Status": "Start",
    "Parameter": "QAM Transmit",
    "Data": {"Value": [], "Index": 0}
}
class consumerThread (threading.Thread):
    def __init__(self, threadID, name, kafkaBroker, ev, numExecutors):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.KafkaConsumer = kafkaConsumer.KafkaConsumerM5(kafkaBroker, 'sequenceComplete')
        self.waitEvent = ev
        self.numExecutors = numExecutors
        self.exit = False
        self.waitEvent.clear()
      
    def run(self):
        print ("Starting " + self.name)
        #print_time(self.name, self.counter, 5)
        try:
            while True:
                if self.exit == True:
                    break
                resultData = self.KafkaConsumer.WaitforSequenceComplete(self.numExecutors)
                print(resultData)
                self.waitEvent.set()
        except KeyboardInterrupt:
            self.exit = True

        print ("Exiting " + self.name)

    def stop(self):
        self.exit = True


class GenerateEvents():
    def __init__(self, kafkaBroker, ev, topic="qamData"):
        self.events = []
        self.kafka = kafka.kProducer(kafkaBroker)
        self.topic = topic
        self.waitForSequenceComplete = ev

    def CreateEvents(self, parameter, data, status = 'Segment'):
    
        numParameters = len(data)
        numEvents = len(data['0']) 
        for i in range(0, numEvents):
            _message = {}
            if i == 0 and status == 'Start':
               _message['Status'] = 'Start'
            elif i == numEvents-1 and status == "End":
                _message['Status'] = 'End'
            else:
                _message['Status'] = 'Segment'
            
            _message['Parameter'] = parameter
            _message['Data'] = {}
            _message['Data']['Value'] = []
            for d in range(0, numParameters):
                _message['Data']['Value'].append(data[str(d)][i])
            _message['Data']['Index'] = i
           
            #self.events.append(json.dumps(_message))
            self.events.append(_message)
    
    def GetEvents(self):
        return self.events
        
    def GenerateQAMEvents(self, QAM):
        self.events = []
        
        fileName1 = 'Transmit_QAM_' + QAM + '.json'
        fileName2 = 'Receive_QAM_' + QAM + '.json'
        fileName3 = 'Segment_QAM_' + QAM + '.json'

        print("Reading: ", fileName1)
        f_txQAM = open(fileName1, "r")
        txQAM = json.loads(f_txQAM.read()) 
        self.CreateEvents("QAM Transmit", txQAM, "Start")
        f_txQAM.close()

        print("Reading: ", fileName2)
        f_rxQAM = open(fileName2, "r")
        rxQAM = json.loads(f_rxQAM.read()) 
        self.CreateEvents("QAM Receive", rxQAM, "Segment")
        f_rxQAM.close()

        print("Reading: ", fileName3)
        f_seqQAM = open(fileName3, "r")
        seqQAM = json.loads(f_seqQAM.read()) 
        self.CreateEvents("Symbol", seqQAM, "End")
        f_seqQAM.close()

        return self.events

    def SendKafkaEvents(self, partition):
        
        print("Num Kafka Events: ", len(self.events))
        self.kafka.SendEvents(self.events, self.topic, partition)
    
    def WaitForNextDut(self, numExecutors):
        self.waitForSequenceComplete.wait()
        self.waitForSequenceComplete.clear()
        #return self.KafkaConsumer.WaitforSequenceComplete(numExecutors)

def GenerateMessageFile(fileName, QAM):

    ev = threading.Event()
    gen = GenerateEvents("localhost:9092", ev)
    events =  gen.GenerateQAMEvents(QAM)
    print(len(events))
    f = open(fileName, "w")
    f.writelines(events)
    f.close()


def GenerateKafkaMessages(kafkaBroker, partitions, numDuts, numExecutors, topic='qamData'):
    #Create event that notifes next DUT measurement
    waitForSequenceComplete = threading.Event()
    consumer = consumerThread(1, "consumerThread", kafkaBroker, waitForSequenceComplete, numExecutors)
    consumer.start()
    #Measurement Data (Events)
    gen = GenerateEvents(kafkaBroker, waitForSequenceComplete, topic)
    try:
        for i in range(0, numDuts):
            print("Performing measurement on DUT#: ", str(i+1))
            
            #QAM 16
            events = []
            events = gen.GenerateQAMEvents("16")
            print(len(events))
            gen.SendKafkaEvents(partitions[0])

            #QAM 64 
            events = []
            events = gen.GenerateQAMEvents("64")
            gen.SendKafkaEvents(partitions[1])
            print(len(events))

            #QAM 256
            events = []
            events = gen.GenerateQAMEvents("256")
            gen.SendKafkaEvents(partitions[2])
            print(len(events))

            #signal consumer thread if this is the last iteration
            if i == (numDuts-1):
                consumer.stop()
            
            gen.WaitForNextDut(numExecutors)
        
        consumer.join()
    
    except KeyboardInterrupt:
        consumer.stop()
    
if(__name__ == '__main__'):
    
    try:
        kafkaBroker = sys.argv[1]
    except:
        print("Using default kafka broker: simfarm14.ni.corp.natinst.com:9092")
        kafkaBroker = "simfarm14.ni.corp.natinst.com:9092"
    try:
        numDuts = sys.argv[2]
    except:
        numDuts = "2"
    
    try:
        topic = sys.argv[3]
    except:
        topic = "qamData"

    
    partitions = [0, 1, 2]
    
    GenerateKafkaMessages(kafkaBroker, partitions, int(numDuts), len(partitions), topic)
    