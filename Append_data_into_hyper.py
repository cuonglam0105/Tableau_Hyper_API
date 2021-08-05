from tableauhyperapi import Connection, HyperProcess, SqlType, TableDefinition, \
    escape_string_literal, escape_name, NOT_NULLABLE, Telemetry, Inserter, CreateMode, TableName
import pandas as pd
import csv
import json

# Khởi động tiến trình HyperProcess
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    print('The Hyper Process has started')


    # Đường dẫn file input và output
    path_to_input_file = r"C:\Users\T450\PycharmProjects\pythonProject\test.json"
    path_to_database= r"C:\Users\T450\PycharmProjects\pythonProject\Jason_to_hyper.hyper"


    # Mở kết nối tới file Hyper
    with Connection(endpoint=hyper.endpoint, database=path_to_database, create_mode = CreateMode.NONE) as connection:
        print("The Connection to the Hyper file is open")


        #Định nghĩa bảng
        table_definition = TableDefinition(table_name= TableName("Schema","Extract"), columns=[
            TableDefinition.Column("Col1", SqlType.text()),
            TableDefinition.Column("Col2", SqlType.big_int())
        ])
        print("The table is defined")


        #Kết nối và định nghĩa dữ liệu cần thực hiện
        df = pd.read_json(path_to_input_file)
        data = df.values


        #Đẩy dữ liệu vào file Hyper
        with Inserter(connection, table_definition) as inserter:
            inserter.add_rows(data)
            inserter.execute()
            print('Adding data to the hyper file...')
        print('Adding data completed')
print('Close all connections and close the HyperProcess')
