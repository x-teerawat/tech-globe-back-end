import os
import pika
from flask import Flask, request, jsonify
import json
import numpy as np
from datetime import datetime
import pandas as pd
import logging
import socket
import ssl

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

# ยิงไปคิว techglobetrading.com

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'b-2a1287bd-b3c1-4c80-aa53-f4bef762b90b.mq.ap-southeast-1.amazonaws.com')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5671))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'ubuntu')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'techglobetrading')

def check_host_resolution(host):
    try:
        socket.gethostbyname(host)
        return True
    except socket.error:
        return False

if not check_host_resolution(RABBITMQ_HOST):
    logging.error(f"You cannot change host {RABBITMQ_HOST} to IP ok!!")
else:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    ssl_options = pika.SSLOptions(ssl.create_default_context(), RABBITMQ_HOST)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials, ssl_options=ssl_options)
    
    try:
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        # กำหนด exchange
        channel.exchange_declare(exchange='trading', durable=True, exchange_type='direct')

        def rabbit(dfjson):
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=RABBITMQ_HOST,
                        port=RABBITMQ_PORT,
                        credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD),
                        ssl_options=ssl_options
                    )
                )
                channel = connection.channel()
                channel.exchange_declare(exchange='trading', durable=True, exchange_type='direct')
                channel.basic_publish(exchange='trading', routing_key='production.trading.bot.order.created.alpaca', body=dfjson)
                logging.info(f"send {dfjson} Done!!")
            except Exception as e:
                logging.error(f"fail to sent API: {e}")
            finally:
                connection.close()

        def getCondition(data, creditOfTheDay, creditRemain, percentSlRule = 0.01):
            try:
                botId = str(data[0])
                stockName = str(data[1])
                timeStr = pd.Timestamp(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).tz_localize("US/Eastern")
                buyPrice = float(data[3])
                takeProfit = float(data[4])
                stopLoss = float(data[5]) # np.round(float(data[5]), 2)
                qty = float(data[6])
                strategy = str(data[7])
                side = str(data[8]) # buy or sell
                type = str(data[9]) # "market", Limit
                broker = str(data[10])
                
                
                # creditOfTheDay = 10000
                # creditRemain = 10000  # 9900

                # percentSlRule = 0.01
                # qty = 20

                # buyPrice = -1
                # stopLoss = 115
                # takeProfit = 121

                # alpacaBuyPrice = 120

                slCreditOfTheDayUSD = float(creditOfTheDay * percentSlRule)

                realSLCreditRemainUSD = float(-(creditOfTheDay - creditRemain)+slCreditOfTheDayUSD)  

                valueUSD = qty*buyPrice

                valueSlUSD = qty*stopLoss

                realSlUSD = valueUSD - valueSlUSD

                slx = ((qty*buyPrice)-realSLCreditRemainUSD)/qty
                percentSLx = stopLoss / buyPrice

                if creditOfTheDay >= valueUSD:
                    if buyPrice < takeProfit:
                        percentTP = takeProfit / buyPrice #  percentTP ต้องเอาค่าไปคูณกับ ราคา Market ที่ได้
                        print(1)
                    else:
                        percentTP = 4 
                        
                    if realSLCreditRemainUSD > 0:
                        print(2)
                        print(f"realSLCreditRemainUSD: {realSLCreditRemainUSD}")
                        print(f"realSlUSD: {realSlUSD}")
                        if realSLCreditRemainUSD > realSlUSD: 
                            print(3)
                            if buyPrice > stopLoss: 
                                print(4) 
                                if (percentSLx < 1): # and (percentSLx > slx):
                                    print(5)
                                    percentSL = stopLoss/buyPrice    
                                                
                                else:
                                    print(6)
                                    percentSL = slx/buyPrice 
                            else:
                                print(7)
                                # percentSL = slx/buyPrice
                                percentSL = 1
                        else:
                            print(8)
                            percentSL = slx/buyPrice
                            # percentSL = 1
                    else:
                        print(9)
                        # percentSL = slx/buyPrice
                        percentSL = 1
                else:
                    percentTP = 1
                    percentSL = 1
                    
                        
                print("##############################################")    
                print(f"creditOfTheDay:{creditOfTheDay} USD")  
                print(f"creditRemainCurrent:{creditRemain} USD")  
                print(f"value:{valueUSD} USD")            
                print(f"percentTP:{percentTP}")        
                print(f"percentSL:{percentSL}") 
                print(f"alpacaBuyPrice: {alpacaBuyPrice} PositionUSD") 
                print(f"alpacastopLoss: {alpacaBuyPrice*percentSL} PositionUSD") 
                print(f"alpacaTakeProfit: {alpacaBuyPrice*percentTP} PositionUSD ") 
                print(f"alpacarealStoploss: {(alpacaBuyPrice*qty)-((alpacaBuyPrice*percentSL)*qty)} USD") 
                
                
                dfDict = {
                    "payload": {      
                        "botId": botId,
                        "stock": stockName,
                        "timestamp": str(timeStr),
                        "buyPrice": buyPrice,   # entry @ marketPrice
                        "stopLoss": percentSL, 
                        "takeProfit": percentTP, 
                        "value": qty,
                        "strategy": strategy,
                        "side": side,
                        "type": type, 
                        "broker": broker
                    }
                }
                dfJson = json.dumps(dfDict)
                rabbit(dfJson)
                logging.info(dfJson)
            except Exception as e:
                logging.error(f"ล้มเหลวในการประมวลผลข้อมูล: {e}")

        @app.route('/api/post', methods=['POST'])
        def receive_post():
            if request.method == 'POST':
                data = [
                    request.form.get('botId'),
                    request.form.get('stockName'),
                    request.form.get('timestamp'),
                    request.form.get('buyPrice'),     # entry point 10USD
                    request.form.get('stopLoss'),     # %  8 USD  20%
                    request.form.get('takeProfit'),   # %
                    request.form.get('qty'),
                    request.form.get('strategy'),
                    request.form.get('side'),
                    request.form.get('type'),
                    request.form.get('broker'),
                ]
                logging.info(f"ได้รับข้อมูล: {data}")
                getCondition(data)
                return jsonify(data), 200

        if __name__ == '__main__':
            app.run(debug=True)

    except Exception as e:
        logging.error(f"ล้มเหลวในการเชื่อมต่อกับ RabbitMQ: {e}")

