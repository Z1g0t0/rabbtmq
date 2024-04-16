import pika
from pika.exchange_type import ExchangeType

def process_pgt(ch, method, properties, body):

    body = body.decode("utf-8")

    print(f'Agencia - Pagamento Recebido\n\t{body}\n')
    channel.basic_publish( exchange='pagamento',
                           routing_key=f"Pagamento.{body}.Aprovado", 
                           body=f"{body}" )

    print(f'Agencia - Pagamento APROVADO\n\t{body}\n')


if __name__ == "__main__":

    params = pika.ConnectionParameters('localhost')
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.exchange_declare( exchange='pagamento', 
                              exchange_type=ExchangeType.topic )

    pagamento_env_q = channel.queue_declare( queue='pagamentos_env', 
                                             exclusive=True )
    channel.queue_bind( exchange='pagamento', 
                        queue=pagamento_env_q.method.queue, 
                        routing_key=f'Pagamento.#.Enviado' )

    pagamento_rec_q = channel.queue_declare(queue='pagamentos_rec', exclusive=True)

    channel.basic_consume( queue=pagamento_env_q.method.queue, auto_ack=True, 
                           on_message_callback=process_pgt)


    print('\nInicio Agencia de Credito\n')

    channel.start_consuming()
