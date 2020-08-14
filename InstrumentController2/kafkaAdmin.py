from kafka import KafkaAdminClient
from kafka.admin import NewTopic

class AdminClient():
    def __init__(self, kafkaBroker):
        self.admin = KafkaAdminClient(bootstrap_servers = kafkaBroker)
    
    def CreateTopics (self, topics, numpartitions=3):

        """ Create topics """
        new_topics = [NewTopic(topic, num_partitions=numpartitions, replication_factor=1) for topic in topics]

        # Call create_topics to asynchronously create topics, a dict
        # of <topic,future> is returned.
        fs = self.admin.create_topics(new_topics)

        for topic in topics:
             print("Topic {} created".format(topic))
        '''
        # Wait for operation to finish.
        # Timeouts are preferably controlled by passing request_timeout=15.0
        # to the create_topics() call.
        # All futures will finish at the same time.

        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} created".format(topic))
            except Exception as e:
                print("Failed to create topic {}: {}".format(topic, e))
        '''
    def DeleteTopics(self, topics):

        """ delete topics """

        # Call delete_topics to asynchronously delete topics, a future is returned.
        # By default this operation on the broker returns immediately while
        # topics are deleted in the background. But here we give it some time (30s)
        # to propagate in the cluster before returning.
        #
        # Returns a dict of <topic,future>.

        fs = self.admin.delete_topics(topics, timeout_ms=30)
        # Wait for operation to finish.
        for topic in topics:
            print("Topic {} deleted".format(topic))
        '''
        for topic, f in fs.items():

            try:
                f.result()  # The result itself is None
                print("Topic {} deleted".format(topic))

            except Exception as e:
                print("Failed to delete topic {}: {}".format(topic, e))
        '''

if __name__ == '__main__':
    import sys
    try:
        topic = sys.argv[1]
    except:
        print("Please Specify topic name")
        sys.exit()
    
    try:
        action = sys.argv[2]
    except:
        print("Please specify action:create/delete")
        sys.exit()
    
    try:
        broker = sys.argv[3]
    except:
        print("Please specify Kafa broker address")
        sys.exit()

    #Create Admin client
    a = AdminClient(broker)
    if action.lower() == 'create':
        a.CreateTopics([topic])
    elif action.lower() == 'delete':
        a.DeleteTopics([topic])
    else:
        print ("unknown action requested")