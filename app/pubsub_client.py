import os
import json
import logging
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from google.auth import default

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PubSubClient:
    def __init__(self):
        """Initialize the Pub/Sub client with proper authentication handling."""
        self.project_id = os.environ.get('PUBSUB_PROJECT_ID')
        self.topic_id = os.environ.get('PUBSUB_TOPIC_ID')
        self.subscription_id = os.environ.get('PUBSUB_SUBSCRIPTION_ID')
        
        if not all([self.project_id, self.topic_id, self.subscription_id]):
            missing = []
            if not self.project_id: missing.append('PUBSUB_PROJECT_ID')
            if not self.topic_id: missing.append('PUBSUB_TOPIC_ID')
            if not self.subscription_id: missing.append('PUBSUB_SUBSCRIPTION_ID')
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        # Initialize clients with fallback authentication
        try:
            # Try to use the credentials file
            creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
            if creds_path and os.path.exists(creds_path):
                logger.info(f"Using credentials from: {creds_path}")
                credentials = service_account.Credentials.from_service_account_file(creds_path)
                self.publisher = pubsub_v1.PublisherClient(credentials=credentials)
                self.subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
            else:
                # Fall back to default authentication
                logger.warning(
                    f"Credentials file not found at {creds_path if creds_path else 'GOOGLE_APPLICATION_CREDENTIALS not set'}, "
                    "using default authentication"
                )
                credentials, project = default()
                self.publisher = pubsub_v1.PublisherClient(credentials=credentials)
                self.subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
                
                # If project_id wasn't provided, use the one from default credentials
                if not self.project_id:
                    self.project_id = project
                    logger.info(f"Using project ID from default credentials: {self.project_id}")
        except Exception as e:
            logger.error(f"Error initializing Pub/Sub clients: {str(e)}")
            raise
        
        # Get the full topic and subscription paths
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
        self.subscription_path = self.subscriber.subscription_path(
            self.project_id, self.subscription_id
        )
        
        logger.info(f"PubSub client initialized for project: {self.project_id}")
        logger.info(f"Topic: {self.topic_id}, Subscription: {self.subscription_id}")
    
    def publish_message(self, message_data):
        """Publish a message to the topic."""
        try:
            if isinstance(message_data, dict):
                message_data = json.dumps(message_data).encode('utf-8')
            elif isinstance(message_data, str):
                message_data = message_data.encode('utf-8')
            elif not isinstance(message_data, bytes):
                raise TypeError("Message must be a dict, string, or bytes")
                
            future = self.publisher.publish(self.topic_path, data=message_data)
            message_id = future.result()
            logger.info(f"Published message with ID: {message_id}")
            return message_id
        except Exception as e:
            logger.error(f"Error publishing message: {str(e)}")
            raise
    
    def create_subscription_if_not_exists(self):
        """Create the subscription if it doesn't exist."""
        try:
            self.subscriber.get_subscription(subscription=self.subscription_path)
            logger.info(f"Subscription {self.subscription_id} already exists")
        except Exception:
            logger.info(f"Creating subscription {self.subscription_id}")
            self.subscriber.create_subscription(
                name=self.subscription_path, topic=self.topic_path
            )
            logger.info(f"Subscription {self.subscription_id} created")
    
    def subscribe_to_messages(self, callback):
        """Subscribe to messages with a callback function."""
        try:
            self.create_subscription_if_not_exists()
            
            def callback_wrapper(message):
                try:
                    logger.info(f"Received message ID: {message.message_id}")
                    callback(message)
                    message.ack()
                    logger.info(f"Acknowledged message ID: {message.message_id}")
                except Exception as e:
                    logger.error(f"Error processing message {message.message_id}: {str(e)}")
                    # Don't ack the message so it can be retried
            
            logger.info(f"Starting subscription to {self.subscription_id}")
            streaming_pull_future = self.subscriber.subscribe(
                self.subscription_path, callback=callback_wrapper
            )
            logger.info("Subscription started successfully")
            return streaming_pull_future
        except Exception as e:
            logger.error(f"Error subscribing to messages: {str(e)}")
            raise