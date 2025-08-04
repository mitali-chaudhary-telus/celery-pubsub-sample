import json
import logging
from app.celery_app import celery_app
from app.pubsub_client import PubSubClient

logger = logging.getLogger(__name__)

@celery_app.task(name='process_pubsub_message')
def process_pubsub_message(message_data):
    """Process a message received from Pub/Sub."""
    try:
        logger.info(f"Processing message: {message_data}")
        # Add your message processing logic here
        return {"status": "success", "message": "Message processed successfully"}
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        return {"status": "error", "message": str(e)}

@celery_app.task(name='start_pubsub_subscription')
def start_pubsub_subscription():
    """Start subscribing to Pub/Sub messages."""
    client = PubSubClient()
    
    def message_callback(message):
        try:
            data = message.data.decode('utf-8')
            logger.info(f"Received message: {data}")
            
            # Process message with Celery task
            process_pubsub_message.delay(data)
        except Exception as e:
            logger.error(f"Error in message callback: {str(e)}")
    
    # Start subscription
    streaming_pull_future = client.subscribe_to_messages(message_callback)
    
    try:
        # Keep the task running to continue receiving messages
        streaming_pull_future.result()
    except Exception as e:
        streaming_pull_future.cancel()
        logger.error(f"Subscription stopped: {str(e)}")