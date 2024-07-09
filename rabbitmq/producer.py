import pika

def send_message(message):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost')) # เชื่อมต่อกับ RabbitMQ
    channel = connection.channel() # สร้างช่องทาง (Channel) ใช้สำหรับการส่งและรับข้อความ
    channel.queue_declare(queue='hello') # ประกาศคิวที่ชื่อว่า hello. ถ้าคิวนี้ยังไม่ถูกสร้างไว้ก่อนหน้านี้ RabbitMQ จะสร้างมันให้
    channel.basic_publish(exchange='',
                          routing_key='hello',
                          body=message) # routing_key='hello' กำหนดคิวที่ข้อความจะส่งไป และส่งข้อความ (message) ไปยังคิว hello โดยไม่ใช้ exchange (exchange='').
    print(" [x] Sent %r" % message) # พิมพ์ข้อความที่ถูกส่งไปยังคิว เพื่อให้ผู้ใช้งานรู้ว่าข้อความถูกส่งเรียบร้อยแล้ว
    connection.close() # ปิดการเชื่อมต่อกับ RabbitMQ server เพื่อไม่ให้การเชื่อมต่อค้างไว้

if __name__ == "__main__":
    send_message('Hello!')
