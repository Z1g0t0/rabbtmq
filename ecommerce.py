import time
import pika
from pika.exchange_type import ExchangeType


def wait_payment(ch, method, properties, body):

    body = body.decode("utf-8")
    #body 
    print(f"\n\nEcommerce - Entrada de pedido: \n\t{body} \nAguardando pagamento...")
    channel.basic_consume( queue=pagamentos_q.method.queue, auto_ack=True, 
                           on_message_callback=process_order )
    
def process_order(ch, method, properties, body):

    body = body.decode("utf-8")
    print("============================================")
    print(f"Ecommerce - Pagamento CONFIRMADO \n\t{body}")
    print(f"---> Enviado")


if __name__ == "__main__":

    params = pika.ConnectionParameters('localhost')
    connection = pika.BlockingConnection(params)
    channel = connection.channel()


    channel.exchange_declare(exchange='pedido', exchange_type=ExchangeType.topic)

    pedidos_q = channel.queue_declare(queue='pedidos', exclusive=True)
    channel.queue_bind( exchange='pedido', queue=pedidos_q.method.queue, 
                        routing_key="Pedido.#")

    channel.basic_consume( queue=pedidos_q.method.queue, auto_ack=True, 
                           on_message_callback=wait_payment)

    pagamentos_q = channel.queue_declare(queue='pagamentos_apr', exclusive=True)
    channel.queue_bind( exchange='pagamento', queue=pagamentos_q.method.queue, 
                        routing_key=f"Pagamento.#.Aprovado")

    print("\nInicio Ecommerce")
    channel.start_consuming()

