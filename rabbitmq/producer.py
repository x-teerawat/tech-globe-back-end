import pika

def send_message(message):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='hello')
    channel.basic_publish(exchange='',
                          routing_key='hello',
                          body=message)
    print(" [x] Sent %r" % message)
    connection.close()

if __name__ == "__main__":
    send_message('Hello!')
