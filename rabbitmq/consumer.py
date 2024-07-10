import pika
import ssl
import json

def callback(ch, method, properties, body):
    message = json.loads(body)
    print(" [x] Received %r" % message)

def receive_message():
    url = 'b-0e12009e-43dd-49ba-8ba1-7657911bb4f9.mq.ap-southeast-1.amazonaws.com'
    port = 5671
    username = 'ubuntu'
    password = 'techglobetrading'
    
    credentials = pika.PlainCredentials(username, password)
    context = ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH)
    
    parameters = pika.ConnectionParameters(
        host=url,
        port=port,
        virtual_host='/',
        credentials=credentials,
        ssl_options=pika.SSLOptions(context)
    )
    
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    # ประกาศคิวพร้อมคุณสมบัติ durable=True
    channel.queue_declare(queue='trading.robot', durable=True)
    
    # ตั้งค่า consumer เพื่อรอรับข้อความจากคิว
    channel.basic_consume(queue='trading.robot',
                          on_message_callback=callback,
                          auto_ack=True)
    
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == "__main__":
    receive_message()
