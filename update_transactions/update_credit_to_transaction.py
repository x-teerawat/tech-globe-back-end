from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime, time as dtime
import schedule
import pandas as pd
import numpy as np
import time
from bson.objectid import ObjectId

class UpdateCreditToTransaction():
    def __init__(self):
        ### สร้างการเชื่อมต่อกับ MongoDB
        client = MongoClient(f"mongodb://ubuntu:techglobetrading@13.229.230.27:27017")
        
        ### Database
        tg_database = "tg"
        tg_back_end_database = "tg-back-end"
        
        ### Collections
        accounts_collection = "accounts"
        transactions_collection = "transactions"
        credit_info_collection = "credit-info"
        
        ### tg database
        _tg_database = client[tg_database] # Select/Create database
        self.accounts_of_tg = _tg_database[accounts_collection] # Select/Create collection
        self.transactions_of_tg = _tg_database[transactions_collection] # Select/Create collection
        
        ### tg-back-end database
        _tg_back_end_database = client[tg_back_end_database] # Select/Create database
        self.credit_info_of_tg_back_end = _tg_back_end_database[credit_info_collection] # Select/Create collection
        self.transactions_of_tg_back_end = _tg_back_end_database[transactions_collection] # Select/Create collection

        try:
            # พยายามดึงข้อมูลเซิร์ฟเวอร์
            client.admin.command('ping')
            print("Connection successful.")
        except ConnectionFailure:
            print("Server not available.")   
            
        print()
        print("-"*50)
        print() 
            
    ### Get account id
    def get_account_id(self):
        projection = {"account_id": 1}
        self.list_account_ids = [i['_id'] for i in self.accounts_of_tg.find(projection=projection)]
        
        # print(f"All Account Id: {self.list_account_ids}")
        
        # print()
        # print("-"*50)
        # print()
        
    ### Get last date
    def get_last_date(self):
        # Find the document with the last date
        last_date_document = self.credit_info_of_tg_back_end.find_one(sort=[("date", -1)])
        if last_date_document:
            self.last_date = last_date_document.get("date")
            
        else:
            print("No documents found in collection.")
            self.last_date =  None
            
        # print(f"Last date: {self.last_date}")  
        
        # print()
        # print("-"*50)
        # print()
        
    def compare_n_transactions_between_tg_and_tg_back_end(self):
        n_transactions_in_tg = self.transactions_of_tg.count_documents({})
        n_transactions_in_tg_back_end = self.transactions_of_tg_back_end.count_documents({})
        
        if n_transactions_in_tg > n_transactions_in_tg_back_end:
            self.IsUpdateTransaction = True
        elif n_transactions_in_tg == n_transactions_in_tg_back_end:
            self.IsUpdateTransaction = False
        else:
            print(f"n_transactions_in_tg ({n_transactions_in_tg}) < n_transactions_in_tg_back_end ({n_transactions_in_tg_back_end})")
            self.IsUpdateTransaction = None
        
        print(f"n_transactions_in_tg: {n_transactions_in_tg}")
        print(f"n_transactions_in_tg_back_end: {n_transactions_in_tg_back_end}")
        print(f"IsUpdateTransaction: {self.IsUpdateTransaction}, [{datetime.now()}]")
         
        print()
        print("-"*50)
        print()

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
        
        # print(f"Dict initial credit: {self.dict_initial_credits}")
        
        # print()
        # print("-"*50)
        # print()
        
    def check_not_updated_transaction_id(self):
        projection = {"_id": -1}
        transactions_id_of_tg = set([i['_id'] for i in self.transactions_of_tg.find(projection=projection)])
        transactions_id_of_tg_back_end = set([i['_id'] for i in self.transactions_of_tg_back_end.find(projection=projection)])
        self.list_not_updated_transaction_id = list(transactions_id_of_tg - transactions_id_of_tg_back_end)
        
        print(f"List not updated id: {self.list_not_updated_transaction_id}")
            
        print()
        print("-"*50)
        print()
            
    ### Update credit amount to transactions database
    def update_credit_to_transactions(self):
        try:
            for not_updated_transaction_id in self.list_not_updated_transaction_id:
                not_updated_data = self.transactions_of_tg.find_one({"_id": not_updated_transaction_id})
                
                ### Insert the initial credit
                accountId = not_updated_data['accountId']
                initial_credit = self.dict_initial_credits[accountId]
                not_updated_data['initialCredit'] = initial_credit
                
                self.transactions_of_tg_back_end.insert_one(not_updated_data)
                
                print(f"_id: {not_updated_data['_id']}")
                print(f"Insert the credit to the transactions completed, [{datetime.now()}]")
                
        except Exception as e:
            print(f"Account id for the credit updating to the transactions error : {e}")
            
        print()
        print("-"*50)
        print()
        
    def compare_n_closed_status_between_tg_and_tg_back_end(self):
        self.query_closed_status = {'status': 'closed'}
        n_closed_status_in_tg = self.transactions_of_tg.count_documents(self.query_closed_status)
        n_closed_status_in_tg_back_end = self.transactions_of_tg_back_end.count_documents(self.query_closed_status)
        
        if n_closed_status_in_tg > n_closed_status_in_tg_back_end:
            self.IsUpdateStatus = True
        elif n_closed_status_in_tg == n_closed_status_in_tg_back_end:
            self.IsUpdateStatus = False
        else:
            print(f"n_closed_status_in_tg ({n_closed_status_in_tg}) < n_closed_status_in_tg_back_end ({n_closed_status_in_tg_back_end})")
            self.IsUpdateStatus = None
        
        print(f"n_closed_status_in_tg: {n_closed_status_in_tg}")
        print(f"n_closed_status_in_tg_back_end: {n_closed_status_in_tg_back_end}")
        print(f"IsUpdateStatus: {self.IsUpdateStatus}, [{datetime.now()}]")
         
        print()
        print("-"*50)
        print()
        
    def check_not_updated_status_id(self):
        status_id_of_tg = set([i['_id'] for i in self.transactions_of_tg.find(self.query_closed_status)])
        status_id_of_tg_back_end = set([i['_id'] for i in self.transactions_of_tg_back_end.find(self.query_closed_status)])
        self.list_not_updated_status_id = sorted(list(status_id_of_tg - status_id_of_tg_back_end)) # Convert set to list type and ascending sort
        
        print(f"List not updated status id: {self.list_not_updated_status_id}")
        print(f"Amount of not updated status id: {len(self.list_not_updated_status_id)}")
            
        print()
        print("-"*50)
        print()
        
    def update_status(self):
        for not_updated_status_id in self.list_not_updated_status_id:
            updated_status_data = self.transactions_of_tg.find_one({"_id": not_updated_status_id})
            update_status_result = self.transactions_of_tg_back_end.update_one({'_id': not_updated_status_id}, {'$set': updated_status_data})
            print(f'Update status at id: {not_updated_status_id}, modified count: {update_status_result.modified_count}')
            
        print()
        print("-"*50)
        print()
        
        
    def run(self):
        while True:
            ### Check and update transactions
            self.compare_n_transactions_between_tg_and_tg_back_end()
            if self.IsUpdateTransaction:
                self.get_last_initial_credits()
                self.check_not_updated_transaction_id()
                self.update_credit_to_transactions()
                
            ### Check and update statuses
            self.compare_n_closed_status_between_tg_and_tg_back_end()
            if self.IsUpdateStatus:
                self.check_not_updated_status_id()
                self.update_status()
            
            time.sleep(1)
            
            
    ### Determine schedule
    def is_within_time_range(self):
        now = datetime.now()
        current_time = now.time()
        start_time_evening = dtime(20, 30)
        end_time_morning = dtime(5, 0)

        if now.weekday() < 5:  # Monday to Friday
            if start_time_evening <= current_time or current_time <= end_time_morning:
                return True
        elif now.weekday() == 5:  # Saturday
            if current_time <= end_time_morning:
                return True
        return False

    def scheduled_job(self):
        if self.is_within_time_range():
            self.run()
        
if __name__ == "__main__":
    # ### Schedule the job every second
    # schedule.every(1).seconds.do(UpdateCreditToTransaction().scheduled_job)

    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
    
    UpdateCreditToTransaction().run()
