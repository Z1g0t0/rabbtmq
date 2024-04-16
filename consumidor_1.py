import pika
from pika.exchange_type import ExchangeType

def process_msg(ch, method, properties, body):
    print(f"\nConsumidor 1 - Nova mensagem: \n\t{body}")

params = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(params)
channel = connection.channel()

# Direct
channel.exchange_declare(exchange='logs', exchange_type=ExchangeType.direct)
queue_logs = channel.queue_declare(queue='qLogs_1', exclusive=True)

channel.queue_bind(exchange='logs', queue=queue_logs.method.queue, routing_key='info')
channel.queue_bind(exchange='logs', queue=queue_logs.method.queue, routing_key='warning')
channel.queue_bind(exchange='logs', queue=queue_logs.method.queue, routing_key='erro')
channel.basic_consume(queue=queue_logs.method.queue, auto_ack=True, on_message_callback=process_msg)

# Topic
channel.exchange_declare(exchange='topics', exchange_type=ExchangeType.topic)
queue_topics = channel.queue_declare(queue='qTopic_1', exclusive=True)

channel.queue_bind(exchange='topics', queue=queue_topics.method.queue, routing_key='Primeiro.#')
channel.queue_bind(exchange='topics', queue=queue_topics.method.queue, routing_key='*.Segundo.#')
channel.basic_consume(queue=queue_topics.method.queue, auto_ack=True, on_message_callback=process_msg)

print("Inicio Consumidor 1")
channel.start_consuming()