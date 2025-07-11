import pika

# RabbitMQ credentials
RABBITMQ_USER = "guest"
RABBITMQ_PASS = "guest"
RABBITMQ_HOST = "localhost"  # or 'rabbitmq' if using Docker Compose network
RABBITMQ_PORT = 5672
QUEUE_NAME = "my_queue"

# Create connection parameters
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
params = pika.ConnectionParameters(
    host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)

# Connect and publish
connection = pika.BlockingConnection(params)
channel = connection.channel()

# Ensure the queue exists
channel.queue_declare(queue=QUEUE_NAME, durable=True)

# Publish a message
channel.basic_publish(
    exchange='',
    routing_key=QUEUE_NAME,
    body='Hello, RabbitMQ!',
    properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent
)

print("✅ Message sent.")
connection.close()
