import logging
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)

def process_message(message: str):
    logging.info('Processing message: %s', message)
    
    # producer sends the message to test-topic
    producer = KafkaProducer(
        bootstrap_servers='20.52.20.54:9092', 
        value_serializer=lambda v: str(v).encode('utf-8')  
    )

    producer.send('test-topic', value=message)
    producer.flush() 

    logging.info("Message sent to test-topic :)")

if __name__ == "__main__":
    test_message = "Test message from manual call"
    process_message(test_message)
