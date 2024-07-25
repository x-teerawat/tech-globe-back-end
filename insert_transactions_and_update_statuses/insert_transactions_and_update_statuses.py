import pandas as pd
import schedule
import time
from pymongo import MongoClient
from datetime import datetime
from dateutil.relativedelta import *

class InsertTransactionsAndUpdateStatuses():
    def __init__(self):
        ### สร้างการเชื่อมต่อกับ MongoDB
        client = MongoClient(f"mongodb://ubuntu:techglobetrading@13.229.230.27:27017")
        
        ### Database
        tg_database = "tg"
        tg_back_end_database = "tg-back-end"
        
        ### Collections
        transactions_collection = "transactions"
        accounts_collection = "accounts"
        credits_collection = "credits"
        
        ### tg database
        _tg_database = client[tg_database] # Select/Create database
        self.transactions_of_tg = _tg_database[transactions_collection] # Select/Create collection
        
        ### tg-back-end database
        _tg_back_end_database = client[tg_back_end_database] # Select/Create database
        self.transactions_of_tg_back_end = _tg_back_end_database[transactions_collection] # Select/Create collection
        self.accounts_of_tg = _tg_database[accounts_collection] # Select/Create collection
        self.credit_info_of_tg_back_end = _tg_back_end_database[credits_collection] # Select/Create collection

        try:
            # พยายามดึงข้อมูลเซิร์ฟเวอร์
            client.admin.command('ping')
            print("Connection successful.")
        except Exception as e:
            print(f"Server not available: {e}")   
        
        ### Get last initial credits
        self.get_last_initial_credits()
        print()
        print("-"*50)
        print()
        
    ### Get account id
    def get_account_id(self):
        projection = {"account_id": 1}
        self.list_account_ids = [i['_id'] for i in self.accounts_of_tg.find(projection=projection)]
        
    ### Get last date
    def get_last_date(self):
        # Find the document with the last date
        last_date_document = self.credit_info_of_tg_back_end.find_one(sort=[("date", -1)])
        if last_date_document:
            self.last_date = last_date_document.get("date")
        else:
            print("No documents found in collection.")
            self.last_date =  None
        
    ### Get last initial credits
    def get_last_initial_credits(self):
        self.get_account_id()
        self.get_last_date()
        
        query = {
            "$and": [
                {"accountId": {"$in": self.list_account_ids}},
                {"date": self.last_date},
            ]
        }
        accounts_data = self.credit_info_of_tg_back_end.find(query)
        self.dict_initial_credits = {i["accountId"]:i["initialCredit"] for i in accounts_data}

    # สร้างฟังก์ชันเพื่อเปรียบเทียบข้อมูล
    def compare_collections(self):
        transactions_of_tg_data = list(self.transactions_of_tg.find({}))
        transactions_of_tg_back_end_data = list(self.transactions_of_tg_back_end.find({}))
        
        # เปรียบเทียบเอกสารทีละเอกสาร
        transactions_of_tg_data_ids = {doc['_id']: doc for doc in transactions_of_tg_data}
        transactions_of_tg_back_end_data_ids = {doc['_id']: doc for doc in transactions_of_tg_back_end_data}
        
        all_transaction_ids = sorted(list(set(transactions_of_tg_data_ids.keys()).union(transactions_of_tg_back_end_data_ids.keys())))
        
        # เปรียบเทียบจำนวนเอกสาร
        if len(transactions_of_tg_data) != len(transactions_of_tg_back_end_data):
            print("len(transactions_of_tg_data) != len(transactions_of_tg_back_end_data)")
            for transaction_id in all_transaction_ids:
                transactions_of_tg_doc = transactions_of_tg_data_ids.get(transaction_id)
                transactions_of_tg_back_end_doc = transactions_of_tg_back_end_data_ids.get(transaction_id)
                
                ### Similarity check
                if transactions_of_tg_doc != transactions_of_tg_back_end_doc:
                    if transactions_of_tg_back_end_doc is None:
                        ### เพิ่มเอกสารใหม่
                        transactions_of_tg_doc['initialCredit'] = self.dict_initial_credits[transactions_of_tg_doc['accountId']]
                        self.transactions_of_tg_back_end.insert_one(transactions_of_tg_doc)
                        print(f"Inserted document with transaction_id: {transaction_id} [{datetime.now()}]")
        else:
            print(f"len(transactions_of_tg_data) == len(transactions_of_tg_back_end_data) [{datetime.now()}]")
            for transaction_id in all_transaction_ids:
                transactions_of_tg_doc = transactions_of_tg_data_ids.get(transaction_id)
                transactions_of_tg_back_end_doc = transactions_of_tg_back_end_data_ids.get(transaction_id)
                
                ### Status check
                if transactions_of_tg_doc['status'] != transactions_of_tg_back_end_doc['status']:
                    ### อัพเดตเอกสาร
                    self.transactions_of_tg_back_end.update_one(
                        {'_id': transaction_id},
                        {'$set': transactions_of_tg_doc}
                    )
                    print(f"Updated document with transaction_id: {transaction_id} [{datetime.now()}]")
                    
        print()

    ### ฟังก์ชันที่จะรันเป็น schedule
    def job(self):
        start_date = datetime.now()
        end_date = pd.to_datetime((datetime.now() + relativedelta(days=1)).strftime('%Y-%m-%d') + ' 05:00:00')
        
        while True:
            # now = datetime.now().time()
            # if now >= datetime.strptime("20:30", "%H:%M").time():
            #     self.compare_collections()
            # if now > end_date:
            #     print(f"Ending job at {now}")
            #     break
            # time.sleep(1)
            # self.compare_collections()
            
            now = datetime.now()
            if now < end_date:
                self.compare_collections()
            else:
                print(f"Ending job at {now}")
                break
            time.sleep(1)

    ### ตั้งค่า schedule จันทร์-ศุกร์ 20:30 ถึง 05:00
    def schedule_jobs(self):
        schedule.every().monday.at("20:30").do(self.job)
        schedule.every().tuesday.at("20:30").do(self.job)
        schedule.every().wednesday.at("20:30").do(self.job)
        schedule.every().thursday.at("20:30").do(self.job)
        schedule.every().friday.at("20:30").do(self.job)
        
        while True:
            schedule.run_pending()
            time.sleep(1)

if __name__ == "__main__":
    # InsertTransactionsAndUpdateStatuses().job()
    InsertTransactionsAndUpdateStatuses().schedule_jobs()
    
    # start_date = datetime.now()
    # end_date = pd.to_datetime((datetime.now() + relativedelta(days=1)).strftime('%Y-%m-%d') + ' 05:00:00')
    # end_date = pd.to_datetime((datetime.now() - relativedelta(days=1)).strftime('%Y-%m-%d') + ' 05:00:00')
    # print(f"start_date: {start_date}")
    # print(f"end_date: {end_date}")
    # print(f"start_date>end_date: {start_date>end_date}")
    # print(f"datetime.strptime('05:00', '%H:%M').time(): {datetime.strptime('05:00', '%H:%M').time()}")
