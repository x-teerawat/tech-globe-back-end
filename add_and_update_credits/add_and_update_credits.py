import asyncio
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime
import schedule

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

async def find_max_date():
    # Select/Create database
    _tg_back_end_database = client[tg_back_end_database]

    # Select/Create collection
    _credit_info_collection = _tg_back_end_database[credit_info_collection]

    # Find the document with the maximum date
    max_date_document = _credit_info_collection.find_one(sort=[("date", -1)])
    if max_date_document:
        max_date = max_date_document.get("date")
        print(f"Maximum date in collection: {max_date}")
        return max_date
    else:
        print("No documents found in collection.")
        return None

async def add_daily_initial_credit():
    # tg database
    _tg_database = client[tg_database]  # Select/Create database
    _accounts_collection = _tg_database[accounts_collection]  # Select/Create collection

    # tg-back-end database
    _tg_back_end_database = client[tg_back_end_database]  # Select/Create database
    _credit_info_collection = _tg_back_end_database[credit_info_collection]  # Select/Create collection

    # Get data from collection
    projection = {"_id": 1, "creditAmount": 1}
    accounts_data = _accounts_collection.find(projection=projection)

    ### Insert credit amount to database
    try:
        for account in accounts_data:
            ### Add initial date
            account['date'] = initial_date
            _credit_info_collection.insert_one(account)

        print("Insert completed")
    except Exception as e:
        print(f"Error to insert: {e}")

async def update_credit_remaining():
    # tg database
    _tg_database = client[tg_database]  # Select/Create database
    _accounts_collection = _tg_database[accounts_collection]  # Select/Create collection

    # tg-back-end database
    _tg_back_end_database = client[tg_back_end_database]  # Select/Create database
    _credit_info_collection = _tg_back_end_database[credit_info_collection]  # Select/Create collection

    # Get data from collection
    projection = {"_id": 1, "creditRemaining": 1}
    accounts_data = _accounts_collection.find(projection=projection)

    ### Add initial date to each account
    try:
        for account in accounts_data:
            account_id = account['_id']
            credit_remaining = account['creditRemaining']
            max_date = await find_max_date()

            ### Update the credit remaining
            result = _credit_info_collection.update_one(
                {"_id": account_id, "date": max_date},
                {"$set": {"creditRemaining": credit_remaining}}
            )
        print("Update completed")
    except Exception as e:
        print(f"Error to update: {e}")

def run_schedule():
    # ตั้งเวลางานให้รันทุกวันจันทร์-ศุกร์ ตอน 2 ทุ่ม
    schedule.every().monday.at("20:30").do(lambda: asyncio.create_task(add_daily_initial_credit()))
    schedule.every().tuesday.at("20:30").do(lambda: asyncio.create_task(add_daily_initial_credit()))
    schedule.every().wednesday.at("20:30").do(lambda: asyncio.create_task(add_daily_initial_credit()))
    schedule.every().thursday.at("20:30").do(lambda: asyncio.create_task(add_daily_initial_credit()))
    schedule.every().friday.at("20:30").do(lambda: asyncio.create_task(add_daily_initial_credit()))

    while True:
        schedule.run_pending()
        time.sleep(1)

async def main():
    # ลูปรัน update_credit_remaining ทุก 0.1 วินาที
    while True:
        await update_credit_remaining()
        await asyncio.sleep(0.1)

# Run the schedule in a separate thread
import threading
threading.Thread(target=run_schedule).start()

# Run the main async function
asyncio.run(main())
