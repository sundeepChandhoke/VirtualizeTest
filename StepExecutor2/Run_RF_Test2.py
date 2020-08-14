import math
import sys

def ComputeQAMData(QAMData, Symbol):

    minIndexList = []
    minValues = []
    for qam in QAMData:
        Q = qam[0]
        I = qam[1]
        D_list = []
        for sym in Symbol:
            Q0 = sym[0]
            I0 = sym[1]

            D=((Q-Q0)**2+(I-I0)**2)**(1/2)
            D_list.append(D)
        
        minIndexList.append(D_list.index(min(D_list)))
        minValues.append(min(D_list))

    return minIndexList, minValues, math.log(len(Symbol), 2)

def int_to_bool_list(num, bits):
    boolList = [bool(num & (1<<n)) for n in range(8)]
    return boolList[:bits]

def bool_list_to_int(boolList):
    num = 0
    for i, val in enumerate (boolList):
        num = num | (int(val)<<i)
    
    return num

def GenerateResultString (QAMData, Symbol):
    minIndexList, minValues, bitsPerElement = ComputeQAMData(QAMData, Symbol)
    boolList = []
    for minIndex in minIndexList:
        boolList = int_to_bool_list(minIndex, int(bitsPerElement)) + boolList
    
    numIter = int(len(boolList)/8)
    #print(numIter)
    sList = boolList
    numList = []
    for iter in range(0, numIter):
        sList2 = sList[:8]
        sList = sList[8:]
        num = bool_list_to_int(sList2)

        numList.append(num)

    resultString = ''.join(chr(i) for i in numList)

    return (resultString)

def GetMeasurementData(partition, kafkaBroker, replay, topic='qamData'):
    import kafkaMessage3Consumer2 as kafka
    import kafkaProducerM5 as kafkaStepComplete

    stepComplete = {
                    "Message": "Step Complete",
                    "StepPartition": 0,
                    "Data": {"Value": [0], "Status": "Pass"}
    }
   
    kc = kafka.KafkaConsumerM3(kafkaBroker, topic, partition, replay)
    kp = kafkaStepComplete.kProducer(kafkaBroker)
    i = 0
    try:
        while True:

            events = kc.GetData()
            if not events:
                break
            i = i+1
            print("Results for DUT# ", str(i))

            QAMTransmit = []
            QAMReceive = []
            Symbol = []
            for event in events:
                elem = event['Data']['Value']
                index = event['Data']['Index']

                if event['Parameter'] == 'QAM Transmit':
                    QAMTransmit.insert(index, elem)

                elif event['Parameter'] == 'QAM Receive':
                    QAMReceive.insert(index, elem)
                
                elif event['Parameter'] == 'Symbol':
                    Symbol.insert(index, elem)
            

            resultsString = GenerateResultString(QAMReceive, Symbol)
            print (resultsString)
            stepComplete['StepPartition'] = partition
            stepComplete['Data']['Status'] = "Pass"
            events = []
            events.append(stepComplete)
            kp.SendEvents(events, "sequenceComplete")
            
    except KeyboardInterrupt:
        pass
    #return QAMTransmit, QAMReceive, Symbol     

if(__name__ == '__main__'):
    #import message3Consumer as mi3
    
    #qamTransmit, qamReceive, symbol = mi3.ReadMessagesFromFile("temp.json")
    import sys

    try:
        partition = sys.argv[1]
    except:
        print("Please Specify partition")
        sys.exit()
    
    try:
        kafkaBroker = sys.argv[2]
    except:
        print("Please Specify kafka broker address")
        print("using default: simfarm14.ni.corp.natinst.com:9092")
        kafkaBroker = "simfarm14.ni.corp.natinst.com:9092"
    
    try:
        action = sys.argv[3]
    except:
        action = 'live'
    
    try:
        topic = sys.argv[4]
    except:
        topic = 'qamData'


    if action.lower() == 'replay':
        replay = True
    else:
        replay = False
    
    GetMeasurementData(int(partition), kafkaBroker, replay, topic)

