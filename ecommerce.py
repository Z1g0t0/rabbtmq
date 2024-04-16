import random
import os
import pika
from pika.exchange_type import ExchangeType

c_ads = {
    'categoria_1'      :   ['Produtos categoria 1...'],
    'categoria_2'      :   ['Produtos categoria 2...'],
    'categoria_3'      :   ['Produtos categoria 3...'],
    'categoria_4'      :   ['Produtos categoria 4...']
    'categoria_5'      :   ['Produtos categoria 5...']
}

params = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(params)
channel = connection.channel()

channel.exchange_declare(exchange='ads', exchange_type=ExchangeType.direct)

while True:

    #os.system('cls')
    #os.system('clear')

    r = random.randint(0,4)

    channel.basic_publish(exchange='ads',
    routing_key=f'Categoria {r}', body=c_ads[r])
    

connection.close()
