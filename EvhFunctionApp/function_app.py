import azure.functions as func
import logging

app = func.FunctionApp()

@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="dico-eventhub",
                               connection="NAMESPACE_CONNECTION_STRING") 
def dicoazf(azeventhub: func.EventHubEvent):
    message = azeventhub.get_body().decode('utf-8')
    logging.info('Python EventHub trigger processed an event: %s', message)
    print(f"Received event: {message}")