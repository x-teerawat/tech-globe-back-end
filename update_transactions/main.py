from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime
import schedule
import pandas as pd
import numpy as np
import time
from bson.decimal128 import Decimal128

class UpdateTransactions():
    def __init__(self):
        ### สร้างการเชื่อมต่อกับ MongoDB
        client = MongoClient(f"mongodb://ubuntu:techglobetrading@13.229.230.27:27017")
        
        tg_database = "tg"
        tg_back_end_database = "tg-back-end"
        accounts_collection = "accounts"
        transactions_collection = "transactions"
        
        ### tg database
        _tg_database = client[tg_database] # Select/Create database
        self.tg_database_accounts_collection = _tg_database[accounts_collection] # Select/Create collection
        self.transactions_of_tg = _tg_database[transactions_collection] # Select/Create collection
        
        ### tg-back-end database
        _tg_back_end_database = client[tg_back_end_database] # Select/Create database
        self.transactions_of_tg_back_end = _tg_back_end_database[transactions_collection] # Select/Create collection

        try:
            # พยายามดึงข้อมูลเซิร์ฟเวอร์
            client.admin.command('ping')
            print("Connection successful.")
        except ConnectionFailure:
            print("Server not available.")      

    # ### Get credit dict from collection
    # def get_credit_dict(self):
    #     projection = {"_id": 1, "creditAmount": 1}
    #     accounts_data = self.tg_database_accounts_collection.find(projection=projection)
    #     credit_dict = {}
    #     try:
    #         for account in accounts_data:
    #             credit_amount = float(account['creditAmount'].to_decimal())
    #             credit_dict[account['_id']] = credit_amount
                
    #         print(f"[{datetime.now()}], Credit data got")
    #     except Exception as e:
    #         print("[{datetime.now()}], Error to insert: {e}")
            
    #     return  credit_dict

    # ### Update credit amount to transactions database
    # def update_credit_to_transactions(self):
    #     credit_dict = self.get_credit_dict()
        
    #     projection = {"_id": 1, "accountId": 1, "status": 1, "createdAt": 1}
    #     transactions_data = self.transactions_of_tg_back_end.find(projection=projection)

    #     try:
    #         for transaction in transactions_data:
    #             ### Add initial date
    #             transaction_id = transaction['_id']
    #             account_id = transaction['accountId']
    #             credit_amount_by_account_id = credit_dict[account_id]
                
    #             ### Update the credit
    #             result = self.transactions_of_tg_back_end.update_one(
    #                 {"_id": transaction_id, "accountId": account_id},
    #                 {"$set": {"initialCredit": credit_amount_by_account_id}}
    #             )
                
    #         print(f"[{datetime.now()}], Update credit amount to transactions in tg-back-end database completed")
    #     except Exception as e:
    #         print("[{datetime.now()}], Error to insert: {e}")
            
    def compare_n_transactions_between_tg_and_tg_back_end(self):
        n_transactions_in_tg = self.transactions_of_tg.count_documents({})
        n_transactions_in_tg_back_end = self.transactions_of_tg_back_end.count_documents({})
        print(f"n_transactions_in_tg: {n_transactions_in_tg}")
        print(f"n_transactions_in_tg_back_end: {n_transactions_in_tg_back_end}")
if __name__ == "__main__":
    # UpdateTransactions().update_credit_to_transactions()
    UpdateTransactions().compare_n_transactions_between_tg_and_tg_back_end()