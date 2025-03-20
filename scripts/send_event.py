from azure.eventhub import EventHubProducerClient, EventData
from datetime import timedelta
from dotenv import load_dotenv
import os

load_dotenv()

CONNECTION_STRING = os.getenv("CONNECTION_STRING")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

# create a producer client
producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STRING, 
    eventhub_name=EVENT_HUB_NAME,
    connection_timeout=timedelta(seconds=60)  # set timeout
)

event_data_batch = producer.create_batch()
event_data_batch.add(EventData("Test event"))
producer.send_batch(event_data_batch)
producer.close()