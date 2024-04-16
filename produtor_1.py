import os
import pika
from pika.exchange_type import ExchangeType

direct = {
    'info'      :   'Log info...    ',
    'warning'   :   'Log warning... ',
    'erro'      :   'Log erro...    '
}

params = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(params)
channel = connection.channel()

channel.exchange_declare(exchange='logs', exchange_type=ExchangeType.direct)

while True:

    #os.system('cls')
    #os.system('clear')

    level = input("\nTipo de log: \n\n\t(1) - Info\n\t(2) - Warning\n\t(3) - Erro\n\t(-1) - Sair\n")

    if not level.isnumeric(): print("Input invalido")
    
    elif level == '-1': break
    
    else:
        level = list(direct.keys())[int(level)-1]
        channel.basic_publish(exchange='logs', routing_key=level, body=direct[level])
    
    level = '0'

connection.close()
