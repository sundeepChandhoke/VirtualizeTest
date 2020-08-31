import json
import sys
import kafkaMessage3Producer as kp
import kafkaMessage2Consumer as kc

message3 = {
    "Status": "Start",
    "Parameter": "QAM Transmit",
    "Data": {"Value": [], "Index": 0}
}

class GenerateEvents():
    def __init__(self, kafkaBroker, topic="qamData"):
        self.events = []
        self.kp = kp.kProducer(kafkaBroker)
        self.kc = kc.KafkaConsumerM2(kafkaBroker, "sequenceIter")
        self.topic = topic
       
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
        #**************Send Event 3 *****************************
        self.kp.SendEvents(self.events, self.topic, partition)

    def WaitForSequenceStart(self):
        #**************Wait for Event 2*************************
        status = self.kc.WaitforSequenceStart()
        return status
    

def GenerateKafkaMessages(kafkaBroker, topic='qamData'):

    gen = GenerateEvents(kafkaBroker, topic)
    #Measurement Data (Events)
    dutNumber = 0
    try:
        while True:
            ev = gen.WaitForSequenceStart()
            print (ev)
            if ev['Message'] == 'Start':
                numPartitions = ev['NumExecutors']
                dutNumber += 1
                print("Performing measurement on DUT#: ", str(dutNumber))

                for i in range(0, numPartitions):
                    events = []
                    if i==0:
                        #QAM 16
                        qam = "16"
                    elif i == 1:
                        #QAM 64 
                        qam = "64"
                    elif i == 2: 
                        #QAM 256
                        qam = "256"
                    else:
                        continue

                    events = gen.GenerateQAMEvents(qam)
                    print(len(events))
                    gen.SendKafkaEvents(i)
            else:
                return
    
    except KeyboardInterrupt:
        return
    
if(__name__ == '__main__'):
    
    try:
        kafkaBroker = sys.argv[1]
    except:
        print("Using default kafka broker: simfarm14.ni.corp.natinst.com:9092")
        kafkaBroker = "simfarm14.ni.corp.natinst.com:9092"
    try:
        topic = sys.argv[2]
    except:
        topic = "qamData"


    GenerateKafkaMessages(kafkaBroker, topic)
    