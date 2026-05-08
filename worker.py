import os
import json
import time
from azure.servicebus import ServiceBusClient
from app import app_graph, projects_collection 

SERVICE_BUS_CONN_STR = os.getenv("SERVICE_BUS_CONN_STR")
QUEUE_NAME = "pipeline-jobs"

def process_job(message_body: dict):
    thread_id = message_body["thread_id"]
    screenplay_text = message_body["screenplay_text"]
    
    # Update status to processing in Cosmos DB
    projects_collection.update_one(
        {"thread_id": thread_id},
        {"$set": {"status": "processing"}}
    )
    
    initial_state = {"screenplay_text": screenplay_text, "current_step": "init"}
    config = {"configurable": {"thread_id": thread_id}}
    
    try:
        print(f"Starting 6-hour LangGraph pipeline for {thread_id}...")
        app_graph.invoke(initial_state, config)
        print(f"Pipeline completed for {thread_id}")
    except Exception as e:
        print(f"Pipeline failed for {thread_id}: {e}")
        projects_collection.update_one(
            {"thread_id": thread_id},
            {"$set": {"status": "failed", "error": str(e)}}
        )

def main():
    print("Worker started, waiting for jobs from Service Bus...")
    # The worker connects to the queue and waits for a message
    with ServiceBusClient.from_connection_string(SERVICE_BUS_CONN_STR) as client:
        with client.get_queue_receiver(queue_name=QUEUE_NAME, max_wait_time=5) as receiver:
            for msg in receiver:
                body = json.loads(str(msg))
                process_job(body)
                # Important: Tell Service Bus the 6-hour job is done so it removes the message
                receiver.complete_message(msg)

if __name__ == "__main__":
    main()