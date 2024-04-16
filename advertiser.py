import random
import time
import pika
from pika.exchange_type import ExchangeType

c_ads = {
    'categoria_1'   :   ['Produtos categoria 1...'],
    'categoria_2'   :   ['Produtos categoria 2...'],
    'categoria_3'   :   ['Produtos categoria 3...'],
    'categoria_4'   :   ['Produtos categoria 4...'],
    'categoria_5'   :   ['Produtos categoria 5...']
}

if __name__ == "__main__":

    params = pika.ConnectionParameters('localhost')
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.exchange_declare(exchange='ads', exchange_type=ExchangeType.direct)

    count = 9
    tempo = 11

    for i in range (0,count):

        r = random.randint(1,5)
        print(f'\nAdvertiser 1 - Enviando AD categoria {r}')

        desc = str(c_ads[f'categoria_{r}'])

        print(desc)

        channel.basic_publish( 
            exchange='ads', 
            routing_key = f'categoria_{r}', 
            body = desc )

        time.sleep(tempo)

    connection.close()
