from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime
import schedule
import pandas as pd
import time
from bson.decimal128 import Decimal128


# ระบุข้อมูลรับรองการเชื่อมต่อกับ MongoDB
# database_name = "tg-back-end"
tg_database = "tg"
accounts_collection = "accounts"

tg_back_end_database = "tg-back-end"
credits_collection = "credits"

# สร้างการเชื่อมต่อกับ MongoDB
client = MongoClient(f"mongodb://ubuntu:techglobetrading@13.229.230.27:27017")

try:
    # พยายามดึงข้อมูลเซิร์ฟเวอร์
    client.admin.command('ping')
    print("Connection successful.")
except ConnectionFailure:
    print("Server not available.")
    
def get_last_date():
    # Select/Create database
    _tg_back_end_database = client[tg_back_end_database]

    # Select/Create collection
    credit_of_tg_back_end = _tg_back_end_database[credits_collection]

    # Find the document with the last date
    last_date_document = credit_of_tg_back_end.find_one(sort=[("date", -1)])
    if last_date_document:
        last_date = last_date_document.get("date")
        return last_date
        
    else:
        print("No documents found in collection.")
        return None
    
def add_daily_initial_credit():
    initial_date = datetime.today().strftime('%Y-%m-%d')
    print(f"initial_date: {initial_date}")
    
    # tg database
    _tg_database = client[tg_database] # Select/Create database
    accounts_of_tg = _tg_database[accounts_collection] # Select/Create collection

    # tg-back-end database
    _tg_back_end_database = client[tg_back_end_database] # Select/Create database
    credit_of_tg_back_end = _tg_back_end_database[credits_collection] # Select/Create collection

    ### Get data from collection
    projection = {"_id": 1, "creditAmount": 1}
    accounts_data = accounts_of_tg.find(projection=projection)

    ### Insert credit amount to database
    try:
        for account in accounts_data:
            ### Add initial date
            account['date'] = initial_date
            
            ### Rename _id to accountId key
            account['accountId'] = account['_id']
            
            ### Convert decimal128 to float
            credit_amount = float(account['creditAmount'].to_decimal())
            account['initialCredit'] = credit_amount
            
            ### Reorder dictionary
            desired_order_list = ['accountId', 'date', 'initialCredit']
            reordered_dict = {k: account[k] for k in desired_order_list}
            
            credit_of_tg_back_end.insert_one(reordered_dict)
        print(f"Insert completed, [{datetime.now()}]")
    except Exception as e:
        print(f"Error to insert: {e}, [{datetime.now()}]")
    
def update_credit_remaining():
    ### tg database
    _tg_database = client[tg_database] # Select/Create database
    accounts_of_tg = _tg_database[accounts_collection] # Select/Create collection

    ### tg-back-end database
    _tg_back_end_database = client[tg_back_end_database] # Select/Create database
    credit_of_tg_back_end = _tg_back_end_database[credits_collection] # Select/Create collection

    # Get data from collection
    projection = {"_id": 1, "creditRemaining": 1}
    accounts_data = accounts_of_tg.find(projection=projection)
    
    ### Add initial date to each account
    try:
        last_date = get_last_date()
        for account in accounts_data:
            account_id = account['_id']
            credit_remaining = account['creditRemaining']
            
            ### Update the credit remaining
            result = credit_of_tg_back_end.update_one(
                {"accountId": account_id, "date": last_date},
                {"$set": {"creditRemaining": credit_remaining}}
            )
        print(f"Update completed, [{datetime.now()}]")
    except Exception as e:
        print(f"Error to update: {e}, [{datetime.now()}]")
    
# ตั้งเวลางานให้รันทุกวันจันทร์-ศุกร์ ตอน 2 ทุ่มครึ่ง
schedule.every().monday.at("20:30").do(add_daily_initial_credit)
schedule.every().tuesday.at("20:30").do(add_daily_initial_credit)
schedule.every().wednesday.at("20:30").do(add_daily_initial_credit)
schedule.every().thursday.at("20:30").do(add_daily_initial_credit)
schedule.every().friday.at("20:30").do(add_daily_initial_credit)

# ตั้งเวลางานให้รันทุกวันอังคาร-เสาร์ ตอนตี 5
schedule.every().tuesday.at("05:00").do(update_credit_remaining)
schedule.every().wednesday.at("05:00").do(update_credit_remaining)
schedule.every().thursday.at("05:00").do(update_credit_remaining)
schedule.every().friday.at("05:00").do(update_credit_remaining)
schedule.every().saturday.at("05:00").do(update_credit_remaining)

# ลูปรัน schedule
while True:
    schedule.run_pending()  # ตรวจสอบงานที่ต้องทำ
    time.sleep(1)  # รอ 1 วินาทีก่อนตรวจสอบงานอีกครั้ง