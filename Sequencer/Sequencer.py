import sys
import kafkaMessage2Producer as kp
import kafkaMessage5Consumer as kc
import time 

class Sequencer ():
    def __init__(self, kafkaBroker, numDUTs, numExecutors):
      
        self.kc = kc.KafkaConsumerM5(kafkaBroker, 'sequenceComplete')
        self.kp = kp.kProducer(kafkaBroker)
        self.numExecutors = numExecutors
        self.numDUTs = numDUTs
      
    def run(self):
        print ("Sequencer Starting ")
        seqTime = []
        #print_time(self.name, self.counter, 5)
        try:
            for i in range(0, self.numDUTs):
                print("Testing DUT# ",  str(i))
                startTime = time.time()
                #***********Send Event 2************
                self.kp.SendStartEvent(self.numExecutors, "sequenceIter")
                #***********Wait for Event 5********
                resultData = self.kc.WaitforSequenceComplete(self.numExecutors)
                stopTime = time.time()
                print(resultData)
                sequenceIterTime = stopTime-startTime
                seqTime.append(sequenceIterTime)
                print("Sequence Iteration Time (s):  ", sequenceIterTime)
            
            import statistics
            print("Average Iteration Time (s):  ", statistics.mean(seqTime))
            self.kp.SendStopEvent("sequenceIter")
        except KeyboardInterrupt:
            print ("Keyboard Interrupt ")
            self.kp.SendStopEvent("sequenceIter")

        print ("Sequencer Exiting ")

def StartSequencer(kafkaBroker, numExecutors, numDUTs):
    sequencer = Sequencer(kafkaBroker,numExecutors, numDUTs)
    sequencer.run()
    
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
        numExecutors = sys.argv[3]
    except:
        numExecutors = "3"
    
    StartSequencer(kafkaBroker, int(numDuts), int(numExecutors))
    