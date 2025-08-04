import os
import json
import logging
from flask import Flask, jsonify
from app.celery_app import celery_app
from app.tasks import start_pubsub_subscription
from app.pubsub_client import PubSubClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "service": "celery-pubsub-worker"})

@app.route('/start-worker', methods=['GET'])
def start_worker():
    """Start the Celery worker and Pub/Sub subscription."""
    try:
        # Start the Pub/Sub subscription task
        task = start_pubsub_subscription.delay()
        return jsonify({
            "status": "success",
            "message": "Celery worker and Pub/Sub subscription started",
            "task_id": task.id
        })
    except Exception as e:
        logger.error(f"Error starting worker: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Failed to start worker: {str(e)}"
        }), 500

@app.route('/publish', methods=['GET'])
def publish_test_message():
    """Publish a test message to Pub/Sub."""
    try:
        client = PubSubClient()
        message_id = client.publish_message({
            "type": "test",
            "content": "This is a test message"
        })
        return jsonify({
            "status": "success",
            "message": "Test message published",
            "message_id": message_id
        })
    except Exception as e:
        logger.error(f"Error publishing message: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Failed to publish message: {str(e)}"
        }), 500

if __name__ == '__main__':
    # Get port from environment variable or default to 8080
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)