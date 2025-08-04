import os
import json
from google.cloud import pubsub_v1
from google.oauth2 import service_account

class PubSubClient:
    def __init__(self):
        self.project_id = os.environ.get('PUBSUB_PROJECT_ID')
        self.topic_id = os.environ.get('PUBSUB_TOPIC_ID')
        self.subscription_id = os.environ.get('PUBSUB_SUBSCRIPTION_ID')
        
        # Initialize clients
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()
        
        # Get the full topic and subscription paths
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
        self.subscription_path = self.subscriber.subscription_path(
            self.project_id, self.subscription_id
        )
    
    def publish_message(self, message_data):
        """Publish a message to the topic."""
        if isinstance(message_data, dict):
            message_data = json.dumps(message_data).encode('utf-8')
        elif isinstance(message_data, str):
            message_data = message_data.encode('utf-8')
            
        future = self.publisher.publish(self.topic_path, data=message_data)
        message_id = future.result()
        return message_id
    
    def create_subscription_if_not_exists(self):
        """Create the subscription if it doesn't exist."""
        try:
            self.subscriber.get_subscription(subscription=self.subscription_path)
        except Exception:
            self.subscriber.create_subscription(
                name=self.subscription_path, topic=self.topic_path
            )
    
    def subscribe_to_messages(self, callback):
        """Subscribe to messages with a callback function."""
        self.create_subscription_if_not_exists()
        
        def callback_wrapper(message):
            callback(message)
            message.ack()
        
        streaming_pull_future = self.subscriber.subscribe(
            self.subscription_path, callback=callback_wrapper
        )
        return streaming_pull_future