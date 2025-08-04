from celery import Celery

# Initialize Celery app
celery_app = Celery(
    'pubsub_tasks',
    broker='memory://',  # Using in-memory broker for Cloud Run
    backend='rpc://'
)

# Load Celery configuration
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    worker_concurrency=1,
)

# Import tasks to ensure they're registered
import app.tasks