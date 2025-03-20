import azure.functions as func
import logging
from kafka import KafkaProducer

app = func.FunctionApp()

@app.function_name(name="KafkaHttpTrigger")
@app.route(route="KafkaHttpTrigger", auth_level=func.AuthLevel.FUNCTION)
def main(req: func.HttpRequest) -> func.HttpResponse:
    """Receives an HTTP request and sends the 'message' parameter to Apache Kafka."""

    message = req.params.get('message')

    if not message:
        return func.HttpResponse("Please provide a message in the query string", status_code=400)

    # Kafka Producer
    try:
        producer = KafkaProducer(
            bootstrap_servers='20.52.20.54:9092',  # vm public ip
            value_serializer=lambda v: str(v).encode('utf-8')  
        )

        # Send message
        producer.send('test-topic', value=message)
        producer.flush()
        logging.info(f"Message sent to Kafka topic: {message}")

        return func.HttpResponse(f"Message '{message}' sent to Kafka!", status_code=200)

    except Exception as e:
        logging.error(f"Failed to send message: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
