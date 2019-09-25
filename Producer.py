import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')
Message = {'Path': 'C:\sqlite3\db\chinook.db', 'CountryName' : 'France', 'Year' : '2011' }
channel.basic_publish(exchange='',
                      routing_key='hello',
                      body= json.dumps(Message),
                      properties=pika.BasicProperties(
                  ))

print(" [x] Sent 'Message'")

connection.close()