import pika
import ssl
import json

def send_message(message):
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
    
    # แปลงข้อมูลเป็น JSON
    message_json = json.dumps(message)
    
    # ส่งข้อความไปยังคิว
    channel.basic_publish(exchange='',
                          routing_key='trading.robot',
                          body=message_json)
    
    print(" [x] Sent %r" % message_json)
    connection.close()

if __name__ == "__main__":
    message = {
        'type': 'greeting',
        'content': 'Hello, RabbitMQ!'
    }
    send_message(message)
