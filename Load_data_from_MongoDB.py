import pandas as pd
from pymongo import MongoClient
import pantab
from tableauhyperapi import TableName
import numpy as np


# Đường dẫn xuất hyper
path_to_hyper = r'C:\Users\T450\Documents\Stock.hyper'
# Đường dẫn file input cần convert
path_to_input_data = r'C:\Users\T450\Documents\outfile.json'

# Tạo kết nối đến server MongoDB
client = MongoClient("mongodb://localhost:27017/")
# Database
db = client["demo"]
# Collection
collection= db["Stock"]


# Tìm documents
data_from_db = collection.find_one({"currentPage" : 1})
print(data_from_db)


# Chuyển documents thành dataframe
df = pd.DataFrame(data_from_db["data"])

print(df)
data = df


# Khai báo schema và table
table = TableName("Schema",'Table')


# Convert dataframe thành Hyper và xuất file
pantab.frame_to_hyper(data,path_to_hyper,table=table, table_mode='a')
#Them thay đổi 
