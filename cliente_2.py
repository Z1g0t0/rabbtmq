import random
import time
import pika
from pika.exchange_type import ExchangeType

cpf = '109876543-21'
pedido_c = 0

def process_msg(ch, method, properties, body):

    global pedido_c, cpf

    body = body.decode("utf-8")

    print(f"\nCliente 2 - Notificacao AD: {body}")

    r = random.randint(0, 12)

    if r % 2 == 0:

        pedido_c += 1

        print(f"Cliente 2 [{cpf}] - Requisitando pedido {pedido_c}")

        r_key = f"Pedido.{pedido_c}.CPF.{cpf}.Requerido"
        body = f"Pedido_numero: {pedido_c}, \n\tComprador_CPF: {cpf} "
        channel.basic_publish( exchange= 'pedido', 
                               routing_key = r_key,
                               body = body )

        channel.basic_publish( exchange='pagamento', 
                               routing_key=f'Pagamento.{r_key}.Enviado', 
                               body=body )


if __name__ == "__main__":

    params = pika.ConnectionParameters('localhost')
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # Direct
    #channel.exchange_declare(exchange='ads', exchange_type=ExchangeType.direct)
    ads_q = channel.queue_declare(queue=f'ads_{cpf}', exclusive=True)

    channel.queue_bind( exchange='ads', queue=ads_q.method.queue, 
                        routing_key='categoria_2' )
    channel.queue_bind( exchange='ads', queue=ads_q.method.queue, 
                        routing_key='categoria_4' )
    channel.queue_bind( exchange='ads', queue=ads_q.method.queue, 
                        routing_key='categoria_5' )

    channel.basic_consume( queue=ads_q.method.queue, auto_ack=True, 
                           on_message_callback=process_msg )


    print("\nInicio Cliente 2 - Categorias 2, 4, 5")

    channel.start_consuming()
