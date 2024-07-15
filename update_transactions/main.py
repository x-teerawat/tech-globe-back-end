from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime
import schedule
import pandas as pd
import numpy as np
import time
from bson.objectid import ObjectId

class UpdateTransactions():
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
        self.list_account_id = [i['_id'] for i in self.accounts_of_tg.find(projection=projection)]
        
        print(f"All Account Id: {self.list_account_id}")
        
        print()
        print("-"*50)
        print()
        
    ### Get last date
    def get_last_date(self):
        # Find the document with the last date
        last_date_document = self.credit_info_of_tg_back_end.find_one(sort=[("date", -1)])
        if last_date_document:
            self.last_date = last_date_document.get("date")
            
        else:
            print("No documents found in collection.")
            self.last_date =  None
            
        print(f"Last date: {self.last_date}")  
        
        print()
        print("-"*50)
        print()
        
    def compare_n_transactions_between_tg_and_tg_back_end(self):
        n_transactions_in_tg = self.transactions_of_tg.count_documents({})
        n_transactions_in_tg_back_end = self.transactions_of_tg_back_end.count_documents({})
        
        if n_transactions_in_tg > n_transactions_in_tg_back_end:
            IsUpdate = True
        elif n_transactions_in_tg == n_transactions_in_tg_back_end:
            IsUpdate = False
        else:
            print(f"n_transactions_in_tg ({n_transactions_in_tg}) < n_transactions_in_tg_back_end ({n_transactions_in_tg_back_end})")
        
        print(f"n_transactions_in_tg: {n_transactions_in_tg}")
        print(f"n_transactions_in_tg_back_end: {n_transactions_in_tg_back_end}")
        print(f"IsUpdate: {IsUpdate}")
         
        print()
        print("-"*50)
        print()

    ### Get last initial credits
    def get_last_initial_credits(self):
        self.get_account_id()
        self.get_last_date()
        
        query = {
            "$and": [
                {"accountId": {"$in": self.list_account_id}},
                {"date": self.last_date},
            ]
        }
        accounts_data = self.credit_info_of_tg_back_end.find(query)
        self.dict_initial_credit = {i["accountId"]:i["creditAmount"] for i in accounts_data}
        
        print(f"Dict initial credit: {self.dict_initial_credit}")
        
        print()
        print("-"*50)
        print()
        
    def check_not_updated_id(self):
        projection = {"_id": 1}
        transactions_id_of_tg = set([i['_id'] for i in self.transactions_of_tg.find(projection=projection)])
        transactions_id_of_tg_back_end = set([i['_id'] for i in self.transactions_of_tg_back_end.find(projection=projection)])
        list_not_updated_id = list(transactions_id_of_tg - transactions_id_of_tg_back_end)
        
        for not_updated_id in list_not_updated_id[:1]:
            not_updated_data = self.transactions_of_tg.find_one({"_id": not_updated_id})
            accountId = not_updated_data['accountId']
            initial_credit = self.dict_initial_credit[accountId]
            print(f"not_updated_data: {not_updated_data}")
            print(f"len(not_updated_data): {len(not_updated_data)}")
            print(f"accountId: {accountId}")
            print(f"initial_credit: {type(initial_credit)}")
            
        print()
        print("-"*50)
        print()
            
    ### Update credit amount to transactions database
    # def update_credit_to_transactions(self):
    #     self.dict_initial_credits = self.get_last_initial_credits()
        
    #     projection = {"_id": 1, "accountId": 1, "status": 1, "createdAt": 1}
    #     transactions_data = self.transactions_of_tg_back_end.find(projection=projection)

    #     try:
    #         for transaction in transactions_data:
    #             ### Add initial date
    #             transaction_id = transaction['_id']
    #             account_id = transaction['accountId']
    #             credit_amount_by_account_id = self.dict_initial_credits[account_id]
                
    #             ### Update the credit
    #             result = self.transactions_of_tg_back_end.update_one(
    #                 {"_id": transaction_id, "accountId": account_id},
    #                 {"$set": {"initialCredit": credit_amount_by_account_id}}
    #             )
                
    #         print(f"[{datetime.now()}], Update credit amount to transactions in tg-back-end database completed")
    #     except Exception as e:
    #         print("[{datetime.now()}], Error to insert: {e}")
        
    def run(self):
        self.compare_n_transactions_between_tg_and_tg_back_end()
        self.get_last_initial_credits()
        self.check_not_updated_id()
        
if __name__ == "__main__":
    UpdateTransactions().run()