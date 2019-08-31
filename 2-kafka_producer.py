from kafka import KafkaProducer
import random
from time import sleep
import sys, os

if __name__=="__main__":

#    if (len(sys.argv) < 4):
#        print("Usage: python 3-kafka_producer.py <low_thres> <high_thres> <topic_name> <data_filename>")
#        print("Suggested: python 3-kafka_producer.py 0.4 0.9 test data/sample_new_users.csv")

    try:
        print("Initialization...")
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
    
        print("Sending messages to kafka 'test' topic...")
#        low = sys.argv[1]
#        high = sys.argv[2]
#        topic = sys.argv[3]
#        filename = sys.argv[4]
    
        f = open('data/occupancy_data.csv', 'rt')
        try:
            for line in f:
                print(line)
                producer.send('test', bytes(line, 'utf8'))
                sleep(random.uniform(float(0.6), float(1.3)))
        finally:
            f.close()
    
        print("Waiting to complete delivery...")
        producer.flush()
        print("End")

    except KeyboardInterrupt:
        print('Interrupted from keyboard, shutdown')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
