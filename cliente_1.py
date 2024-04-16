import pika
from pika.exchange_type import ExchangeType

def process_msg(ch, method, properties, body):
    print(f"\nCliente 1 - Notificacao: \n\t{body}")

params = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(params)
channel = connection.channel()

cpf = '123456789-10'
id = ''

# Direct
channel.exchange_declare(exchange='ads', exchange_type=ExchangeType.direct)
queue_logs = channel.queue_declare(queue=f'ads_{cpf}', exclusive=True)

channel.queue_bind(exchange='ads', queue=queue_logs.method.queue, routing_key='categoria_1')
channel.queue_bind(exchange='ads', queue=queue_logs.method.queue, routing_key='categoria_3')
channel.queue_bind(exchange='ads', queue=queue_logs.method.queue, routing_key='categoria_5')
channel.basic_consume(queue=queue_logs.method.queue, auto_ack=True, on_message_callback=process_msg)

# Topic
channel.exchange_declare(exchange='pedido', exchange_type=ExchangeType.topic)
queue_topics = channel.queue_declare(queue='att_pedido', exclusive=True)
channel.queue_bind(exchange='pedido', queue=queue_topics.method.queue, routing_key=f'Pedido.#.cpf.{cpf}.*')

channel.exchange_declare(exchange='pagamento', exchange_type=ExchangeType.topic)
queue_topics = channel.queue_declare(queue=f'pgto_{cpf}', exclusive=True)
channel.queue_bind(exchange='pgto_{cpf}', queue=queue_topics.method.queue, routing_key=f'Pagamento.efetuado.{cpf}.*')

channel.basic_consume(queue=queue_topics.method.queue, auto_ack=True, on_message_callback=process_msg)

print("Inicio Cliente 1")
channel.start_consuming()