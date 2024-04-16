import os
import pika
from pika.exchange_type import ExchangeType

params = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(params)
channel = connection.channel()

channel.exchange_declare(exchange='topics', exchange_type=ExchangeType.topic)

while True:

    #os.system('cls')
    #os.system('clear')

    bind = input("\nDigite uma sequencia ou -1 para sair: \n\n")

    if bind == '-1': break
    else:
        channel.basic_publish(exchange='topics', routing_key=bind, body=bind)

connection.close()