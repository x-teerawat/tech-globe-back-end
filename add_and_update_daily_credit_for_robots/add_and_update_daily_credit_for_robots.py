from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime
import schedule
import time

# ระบุข้อมูลรับรองการเชื่อมต่อกับ MongoDB
tg_database = "tg"
accounts_collection = "accounts"

tg_back_end_database = "tg-back-end"
credit_info_collection = "credit-info"
initial_date = datetime.today().strftime('%Y-%m-%d')

# สร้างการเชื่อมต่อกับ MongoDB
client = MongoClient(f"mongodb://ubuntu:techglobetrading@13.229.230.27:27017")

try:
    # พยายามดึงข้อมูลเซิร์ฟเวอร์
    client.admin.command('ping')
    print("Connection successful.")
except ConnectionFailure:
    print("Server not available.")
    
def find_last_date(collection_name):
    # Select/Create database
    _tg_back_end_database = client[tg_back_end_database]

    # Select/Create collection
    _collection_name = _tg_back_end_database[collection_name]

    # Find the document with the maximum date
    last_date_document = _collection_name.find_one(sort=[("date", -1)])
    if last_date_document:
        last_date = last_date_document.get("date")
        return last_date
    else:
        print("No documents found in collection.")
        return None
    
def add_daily_initial_credit():
    # tg database
    _tg_database = client[tg_database] # Select/Create database
    _accounts_collection = _tg_database[accounts_collection] # Select/Create collection

    # tg-back-end database
    _tg_back_end_database = client[tg_back_end_database] # Select/Create database
    demo_credit_collection = _tg_back_end_database['demo-credit'] # Select/Create collection
    live_credit_collection = _tg_back_end_database['live-credit'] # Select/Create collection

    # Get data from collection
    projection = {"email": 1, "creditAmount": 1, "isDemo": 1}
    accounts_data = _accounts_collection.find(projection=projection)

    ### Insert credit amount to database
    try:
        for account in accounts_data:
            ### Add initial date
            account['date'] = initial_date
            isDemo = account['isDemo']
            
            ### Reorder dictionary
            desired_order_list = ['email', 'date', 'creditAmount']
            reordered_dict = {k: account[k] for k in desired_order_list}
            
            ### Insert creditAmount to data base
            if isDemo == True:
                demo_credit_collection.insert_one(reordered_dict)
            else:
                live_credit_collection.insert_one(reordered_dict)
            
        print("Insert completed")
    except Exception as e:
        print(f"Error to insert: {e}")


def update_credit_remaining():
    # tg database
    _tg_database = client[tg_database] # Select/Create database
    _accounts_collection = _tg_database[accounts_collection] # Select/Create collection

    # tg-back-end database
    _tg_back_end_database = client[tg_back_end_database] # Select/Create database
    demo_credit_collection = _tg_back_end_database['demo-credit'] # Select/Create collection
    live_credit_collection = _tg_back_end_database['live-credit'] # Select/Create collection

    # Get data from collection
    projection = {"email": 1, "creditRemaining": 1, "isDemo": 1}
    accounts_data = _accounts_collection.find(projection=projection)
    
    ### Add initial date to each account
    try:
        for account in accounts_data:
            email = account['email']
            credit_remaining = account['creditRemaining']
            isDemo = account['isDemo']
            
            ### Update the credit remaining
            if isDemo == True:
                last_date = find_last_date('demo-credit')
                result = demo_credit_collection.update_one(
                    {"email": email, "date": last_date},
                    {"$set": {"creditRemaining": credit_remaining}}
                )
            else:
                last_date = find_last_date('live-credit')
                result = live_credit_collection.update_one(
                    {"email": email, "date": last_date},
                    {"$set": {"creditRemaining": credit_remaining}}
                )
                
            print(f'account, {account}')
        print("Update completed")
    except Exception as e:
        print(f"Error to update: {e}")

# ตั้งเวลางานให้รันทุกวันจันทร์-ศุกร์ ตอน 2 ทุ่มครึ่ง
schedule.every().day.at("20:29").do(add_daily_initial_credit)
schedule.every().day.at("20:30").do(add_daily_initial_credit)

# ลูปรัน schedule และอัพเดทเครดิตคงเหลือ
while True:
    schedule.run_pending()  # ตรวจสอบงานที่ต้องทำ
    update_credit_remaining()  # อัพเดทเครดิตคงเหลือ
    time.sleep(0.1)  # รอ 0.1 วินาทีก่อนตรวจสอบงานอีกครั้ง
