import pika
from pika.exchange_type import ExchangeType

def att_transporte (ch, method, properties, body):

    body = body.decode("utf-8")

    print(f'Transportadora - Inicio \n\t{body}\n')
    channel.basic_publish( exchange='transporte',
                           routing_key=f"Att.Pedido.Transporte.#.Entregue", 
                           body=f"{body}" )

    #print(f'Agencia - Pagamento APROVADO\n\t{body}\n')


if __name__ == "__main__":

    params = pika.ConnectionParameters('localhost')
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.exchange_declare(exchange='transporte', exchange_type=ExchangeType.topic)

    transporte_q = channel.queue_declare( queue='att_transporte', 
                                             exclusive=True )
    channel.queue_bind( exchange='transporte', 
                        queue=transporte_q.method.queue, 
                        routing_key=f'Att.Pedido.Transporte.#.Iniciado' )

    channel.basic_consume( queue=transporte_q.method.queue, auto_ack=True, 
                           on_message_callback= att_transporte)


    print('Inicio Transportador\n')
    channel.start_consuming()
