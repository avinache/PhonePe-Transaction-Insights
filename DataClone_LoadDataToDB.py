import pandas as pd
import numpy as np
import os
import json
import mysql.connector as db
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
import warnings
warnings.filterwarnings('ignore')
print(os.getcwd())

#git clone https://github.com/PhonePe/pulse.git

path = "C:\\Users\\mavin\\Documents\\GUVI\\PythonHandsOn\\pulse\\data\\aggregated\\insurance\\country\\india\\state\\"
Agg_State_List = os.listdir(path)

#Get Aggregated Insurance data from GIT Repository

Agg_Ins_columns={'State':[], 'Year': [], 'Quater': [], 'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []}
for i in Agg_State_List:
    year_i=path+i+"\\"
    Agg_yr_list=os.listdir(year_i)
    for j in Agg_yr_list:
        json_j=year_i+j+"\\"
        Agg_yr_json_list=os.listdir(json_j)
        for k in Agg_yr_json_list:
            fileJson=json_j+k
            Data=open(fileJson, 'r')
            JsonData=json.load(Data)
            for z in JsonData['data'] ['transactionData']:
                Name=z['name']
                count=z['paymentInstruments'][0]['count']
                amount=z['paymentInstruments'][0]['amount']
                Agg_Ins_columns['Transaction_type'].append(Name)
                Agg_Ins_columns['Transaction_count'].append(count)
                Agg_Ins_columns['Transaction_amount'].append(amount)
                Agg_Ins_columns['State'].append(i)
                Agg_Ins_columns['Year'].append(j)
                Agg_Ins_columns['Quater'].append(int(k.strip('.json')))
Agg_Insurance = pd.DataFrame(Agg_Ins_columns)

#Get Aggregated Transaction data from GIT Repository

path = "C:\\Users\\mavin\\Documents\\GUVI\\PythonHandsOn\\pulse\\data\\aggregated\\transaction\\country\\india\\state\\"
Agg_State_List = os.listdir(path)
Agg_trans_columns={'State':[], 'Year': [], 'Quater': [], 'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []}
for i in Agg_State_List:
    year_i=path+i+"\\"
    Agg_yr_list=os.listdir(year_i)
    for j in Agg_yr_list:
        json_j=year_i+j+"\\"
        Agg_yr_json_list=os.listdir(json_j)
        for k in Agg_yr_json_list:
            fileJson=json_j+k
            Data=open(fileJson, 'r')
            JsonData=json.load(Data)
            for z in JsonData['data'] ['transactionData']:
                Name=z['name']
                count=z['paymentInstruments'][0]['count']
                amount=z['paymentInstruments'][0]['amount']
                Agg_trans_columns['Transaction_type'].append(Name)
                Agg_trans_columns['Transaction_count'].append(count)
                Agg_trans_columns['Transaction_amount'].append(amount)
                Agg_trans_columns['State'].append(i)
                Agg_trans_columns['Year'].append(j)
                Agg_trans_columns['Quater'].append(int(k.strip('.json')))
Agg_Transaction = pd.DataFrame(Agg_trans_columns)

#Get Aggregated Users data from GIT Repository

path = "C:\\Users\\mavin\\Documents\\GUVI\\PythonHandsOn\\pulse\\data\\aggregated\\user\\country\\india\\state\\"
Agg_State_List = os.listdir(path)
Agg_user_columns={'State':[], 'Year': [], 'Quater': [], 'RegisteredUsers': [], 'AppOpens': [], 'Brand': [], 'BrandCount': [], 'BrandPercentage': []}
for i in Agg_State_List:
    year_i=path+i+"\\"
    Agg_yr_list=os.listdir(year_i)
    for j in Agg_yr_list:
        json_j=year_i+j+"\\"
        Agg_yr_json_list=os.listdir(json_j)
        for k in Agg_yr_json_list:
            fileJson=json_j+k
            Data=open(fileJson, 'r')
            JsonData=json.load(Data)
            registeredUsers=JsonData['data']['aggregated']['registeredUsers']
            appOpens=JsonData['data']['aggregated']['appOpens']
            #print(i + '-' + j + '-' + k.strip('.json'))
            users_by_device = JsonData['data'].get('usersByDevice', None)
            if users_by_device:
                for z in users_by_device:
                    brand=z['brand']
                    brandCount=z['count']
                    brandPercentage=z['percentage']
                    Agg_user_columns['RegisteredUsers'].append(registeredUsers)
                    Agg_user_columns['AppOpens'].append(appOpens)
                    Agg_user_columns['Brand'].append(brand)
                    Agg_user_columns['BrandCount'].append(brandCount)
                    Agg_user_columns['BrandPercentage'].append(brandPercentage)
                    Agg_user_columns['State'].append(i)
                    Agg_user_columns['Year'].append(j)
                    Agg_user_columns['Quater'].append(int(k.strip('.json')))
            else:
                brand=np.nan
                brandCount=np.nan
                brandPercentage=np.nan
                Agg_user_columns['RegisteredUsers'].append(registeredUsers)
                Agg_user_columns['AppOpens'].append(appOpens)
                Agg_user_columns['Brand'].append(brand)
                Agg_user_columns['BrandCount'].append(brandCount)
                Agg_user_columns['BrandPercentage'].append(brandPercentage)
                Agg_user_columns['State'].append(i)
                Agg_user_columns['Year'].append(j)
                Agg_user_columns['Quater'].append(int(k.strip('.json')))
Agg_Users = pd.DataFrame(Agg_user_columns)
Agg_Users['Brand'] = Agg_Users['Brand'].fillna('NA')
Agg_Users['BrandCount'] = Agg_Users['BrandCount'].fillna(0.0)
Agg_Users['BrandPercentage'] = Agg_Users['BrandPercentage'].fillna(0.0)

#Get MAP Transaction data from GIT Repository

path = "C:\\Users\\mavin\\Documents\\GUVI\\PythonHandsOn\\pulse\\data\\map\\transaction\\hover\\country\\india\\state\\"
Map_State_List = os.listdir(path)
Map_trans_columns={'State':[], 'Year': [], 'Quater': [], 'StateDistrict': [], 'Transaction_count': [], 'Transaction_amount': []}
for i in Map_State_List:
    year_i=path+i+"\\"
    Map_yr_list=os.listdir(year_i)
    for j in Map_yr_list:
        json_j=year_i+j+"\\"
        Map_yr_json_list=os.listdir(json_j)
        for k in Map_yr_json_list:
            fileJson=json_j+k
            Data=open(fileJson, 'r')
            JsonData=json.load(Data)
            for z in JsonData['data'] ['hoverDataList']:
                StateDistrict=z['name']
                count=z['metric'][0]['count']
                amount=z['metric'][0]['amount']
                Map_trans_columns['StateDistrict'].append(StateDistrict)
                Map_trans_columns['Transaction_count'].append(count)
                Map_trans_columns['Transaction_amount'].append(amount)
                Map_trans_columns['State'].append(i)
                Map_trans_columns['Year'].append(j)
                Map_trans_columns['Quater'].append(int(k.strip('.json')))
Map_Transaction = pd.DataFrame(Map_trans_columns)

#Get MAP Users data from GIT Repository

path = "C:\\Users\\mavin\\Documents\\GUVI\\PythonHandsOn\\pulse\\data\\map\\user\\hover\\country\\india\\state\\"
Map_State_List = os.listdir(path)
Map_user_columns={'State':[], 'Year': [], 'Quater': [], 'StateDistrict': [], 'RegisteredUsers': [], 'AppOpens': []}
for i in Map_State_List:
    year_i=path+i+"\\"
    Map_yr_list=os.listdir(year_i)
    for j in Map_yr_list:
        json_j=year_i+j+"\\"
        Map_yr_json_list=os.listdir(json_j)
        for k in Map_yr_json_list:
            fileJson=json_j+k
            Data=open(fileJson, 'r')
            JsonData=json.load(Data)
            hover_data = JsonData['data']['hoverData']
            # Loop through each district and fetch its data
            for district, values in hover_data.items():
                StateDistrict = district
                RegisteredUsers = values['registeredUsers']
                AppOpens = values['appOpens']
                Map_user_columns['RegisteredUsers'].append(RegisteredUsers)
                Map_user_columns['AppOpens'].append(AppOpens)
                Map_user_columns['StateDistrict'].append(StateDistrict)
                Map_user_columns['State'].append(i)
                Map_user_columns['Year'].append(j)
                Map_user_columns['Quater'].append(int(k.strip('.json')))
Map_Users = pd.DataFrame(Map_user_columns)

#Get MAP Insurance data from GIT Repository

path = "C:\\Users\\mavin\\Documents\\GUVI\\PythonHandsOn\\pulse\\data\\map\\insurance\\hover\\country\\india\\state\\"
Map_State_List = os.listdir(path)
Map_ins_columns={'State':[], 'Year': [], 'Quater': [], 'StateDistrict': [], 'Transaction_count': [], 'Transaction_amount': []}
for i in Map_State_List:
    year_i=path+i+"\\"
    Map_yr_list=os.listdir(year_i)
    for j in Map_yr_list:
        json_j=year_i+j+"\\"
        Map_yr_json_list=os.listdir(json_j)
        for k in Map_yr_json_list:
            fileJson=json_j+k
            Data=open(fileJson, 'r')
            JsonData=json.load(Data)
            for z in JsonData['data'] ['hoverDataList']:
                StateDistrict=z['name']
                count=z['metric'][0]['count']
                amount=z['metric'][0]['amount']
                Map_ins_columns['StateDistrict'].append(StateDistrict)
                Map_ins_columns['Transaction_count'].append(count)
                Map_ins_columns['Transaction_amount'].append(amount)
                Map_ins_columns['State'].append(i)
                Map_ins_columns['Year'].append(j)
                Map_ins_columns['Quater'].append(int(k.strip('.json')))
Map_Insurance = pd.DataFrame(Map_ins_columns)

#Get Top Transaction data from GIT Repository

path = "C:\\Users\\mavin\\Documents\\GUVI\\PythonHandsOn\\pulse\\data\\top\\transaction\\country\\india\\state\\"
Top_State_List = os.listdir(path)
Top_Trns_District_columns={'State':[], 'Year': [], 'Quater': [], 'EntityLevel': [], 'EntityValue': [], 'Transaction_count': [], 'Transaction_amount': []}
Top_Trns_Pincode_columns={'State':[], 'Year': [], 'Quater': [], 'EntityLevel': [], 'EntityValue': [], 'Transaction_count': [], 'Transaction_amount': []}
for i in Top_State_List:
    year_i=path+i+"\\"
    Top_yr_list=os.listdir(year_i)
    for j in Top_yr_list:
        json_j=year_i+j+"\\"
        Top_yr_json_list=os.listdir(json_j)
        for k in Top_yr_json_list:
            fileJson=json_j+k
            Data=open(fileJson, 'r')
            JsonData=json.load(Data)
            for z in JsonData["data"]["districts"]:
                EntityLevel = "District"
                EntityValue = z['entityName']
                count=z['metric']['count']
                amount=z['metric']['amount']
                Top_Trns_District_columns['EntityLevel'].append(EntityLevel)
                Top_Trns_District_columns['EntityValue'].append(EntityValue)
                Top_Trns_District_columns['Transaction_count'].append(count)
                Top_Trns_District_columns['Transaction_amount'].append(amount)
                Top_Trns_District_columns['State'].append(i)
                Top_Trns_District_columns['Year'].append(j)
                Top_Trns_District_columns['Quater'].append(int(k.strip('.json')))
            for z in JsonData["data"]["pincodes"]:
                EntityLevel = "Pincode"
                EntityValue = z['entityName']
                count=z['metric']['count']
                amount=z['metric']['amount']
                Top_Trns_Pincode_columns['EntityLevel'].append(EntityLevel)
                Top_Trns_Pincode_columns['EntityValue'].append(EntityValue)
                Top_Trns_Pincode_columns['Transaction_count'].append(count)
                Top_Trns_Pincode_columns['Transaction_amount'].append(amount)
                Top_Trns_Pincode_columns['State'].append(i)
                Top_Trns_Pincode_columns['Year'].append(j)
                Top_Trns_Pincode_columns['Quater'].append(int(k.strip('.json')))
Top_Trns_Dis = pd.DataFrame(Top_Trns_District_columns)
Top_Trns_Pin = pd.DataFrame(Top_Trns_Pincode_columns)
Top_Transaction = pd.concat([Top_Trns_Dis, Top_Trns_Pin], ignore_index=True)

#Get TOP Users data from GIT Repository

path = "C:\\Users\\mavin\\Documents\\GUVI\\PythonHandsOn\\pulse\\data\\top\\user\\country\\india\\state\\"
Top_State_List = os.listdir(path)
Top_user_District_columns={'State':[], 'Year': [], 'Quater': [], 'EntityLevel': [], 'EntityValue': [], 'RegisteredUsers': []}
Top_user_Pincode_columns={'State':[], 'Year': [], 'Quater': [], 'EntityLevel': [], 'EntityValue': [], 'RegisteredUsers': []}
for i in Top_State_List:
    year_i=path+i+"\\"
    Top_yr_list=os.listdir(year_i)
    for j in Top_yr_list:
        json_j=year_i+j+"\\"
        Top_yr_json_list=os.listdir(json_j)
        for k in Top_yr_json_list:
            fileJson=json_j+k
            Data=open(fileJson, 'r')
            JsonData=json.load(Data)
            for z in JsonData["data"]["districts"]:
                EntityLevel = "District"
                EntityValue = z['name']
                RegisteredUsers=z['registeredUsers']
                Top_user_District_columns['EntityLevel'].append(EntityLevel)
                Top_user_District_columns['EntityValue'].append(EntityValue)
                Top_user_District_columns['RegisteredUsers'].append(RegisteredUsers)
                Top_user_District_columns['State'].append(i)
                Top_user_District_columns['Year'].append(j)
                Top_user_District_columns['Quater'].append(int(k.strip('.json')))
            for z in JsonData["data"]["pincodes"]:
                EntityLevel = "Pincode"
                EntityValue = z['name']
                RegisteredUsers=z['registeredUsers']
                Top_user_Pincode_columns['EntityLevel'].append(EntityLevel)
                Top_user_Pincode_columns['EntityValue'].append(EntityValue)
                Top_user_Pincode_columns['RegisteredUsers'].append(RegisteredUsers)
                Top_user_Pincode_columns['State'].append(i)
                Top_user_Pincode_columns['Year'].append(j)
                Top_user_Pincode_columns['Quater'].append(int(k.strip('.json')))
Top_User_Dis = pd.DataFrame(Top_user_District_columns)
Top_User_Pin = pd.DataFrame(Top_user_Pincode_columns)
Top_User = pd.concat([Top_User_Dis, Top_User_Pin], ignore_index=True)

#Get TOP Insurance data from GIT Repository

path = "C:\\Users\\mavin\\Documents\\GUVI\\PythonHandsOn\\pulse\\data\\top\\insurance\\country\\india\\state\\"
Top_State_List = os.listdir(path)
Top_Ins_District_columns={'State':[], 'Year': [], 'Quater': [], 'EntityLevel': [], 'EntityValue': [], 'Transaction_count': [], 'Transaction_amount': []}
Top_Ins_Pincode_columns={'State':[], 'Year': [], 'Quater': [], 'EntityLevel': [], 'EntityValue': [], 'Transaction_count': [], 'Transaction_amount': []}
for i in Top_State_List:
    year_i=path+i+"\\"
    Top_yr_list=os.listdir(year_i)
    for j in Top_yr_list:
        json_j=year_i+j+"\\"
        Top_yr_json_list=os.listdir(json_j)
        for k in Top_yr_json_list:
            fileJson=json_j+k
            Data=open(fileJson, 'r')
            JsonData=json.load(Data)
            for z in JsonData["data"]["districts"]:
                EntityLevel = "District"
                EntityValue = z['entityName']
                count=z['metric']['count']
                amount=z['metric']['amount']
                Top_Ins_District_columns['EntityLevel'].append(EntityLevel)
                Top_Ins_District_columns['EntityValue'].append(EntityValue)
                Top_Ins_District_columns['Transaction_count'].append(count)
                Top_Ins_District_columns['Transaction_amount'].append(amount)
                Top_Ins_District_columns['State'].append(i)
                Top_Ins_District_columns['Year'].append(j)
                Top_Ins_District_columns['Quater'].append(int(k.strip('.json')))
            for z in JsonData["data"]["pincodes"]:
                EntityLevel = "Pincode"
                EntityValue = z['entityName']
                count=z['metric']['count']
                amount=z['metric']['amount']
                Top_Ins_Pincode_columns['EntityLevel'].append(EntityLevel)
                Top_Ins_Pincode_columns['EntityValue'].append(EntityValue)
                Top_Ins_Pincode_columns['Transaction_count'].append(count)
                Top_Ins_Pincode_columns['Transaction_amount'].append(amount)
                Top_Ins_Pincode_columns['State'].append(i)
                Top_Ins_Pincode_columns['Year'].append(j)
                Top_Ins_Pincode_columns['Quater'].append(int(k.strip('.json')))
Top_Ins_Dis = pd.DataFrame(Top_Ins_District_columns)
Top_Ins_Pin = pd.DataFrame(Top_Ins_Pincode_columns)
Top_Insurance = pd.concat([Top_Ins_Dis, Top_Ins_Pin], ignore_index=True)

#Create MySQL connection
user ='PhonePeTransaction'
password='ZXCmnb$3191'
host='localhost'
db_name='phonepetrans'
db_Connection=db.connect(
    host=host,
    user=user,
    password=password,
    database=db_name)
curr=db_Connection.cursor()

#SQL query to create agg_transaction Table in DataBase
sql="""CREATE TABLE agg_transaction
(
State varChar(100),
Year INT,
Quater INT,
Transaction_type varChar(100),
Transaction_count BIGINT,
Transaction_amount DECIMAL(20,6)
);"""
curr.execute(sql)

#insert query
insert="""INSERT INTO agg_transaction
(State, Year, Quater, Transaction_type, Transaction_count, Transaction_amount)
 VALUES (%s, %s, %s, %s, %s, %s)"""
#convert DataFrame rows into a list of tuples
rows=Agg_Transaction.values.tolist()
curr.executemany(insert,rows)
db_Connection.commit()

#SQL query to create agg_insurance Table in DataBase
sql="""CREATE TABLE agg_insurance
(
State varChar(100),
Year INT,
Quater INT,
Transaction_type varChar(100),
Transaction_count BIGINT,
Transacion_amount DECIMAL(20,6)
);"""
curr.execute(sql)

#insert query
insert="""INSERT INTO agg_insurance
(State, Year, Quater, Transacion_type, Transaction_count, Transaction_amount)
 VALUES (%s, %s, %s, %s, %s, %s)"""
#convert DataFrame rows into a list of tuples
rows=Agg_Insurance.values.tolist()
curr.executemany(insert,rows)
db_Connection.commit()

#SQL query to create Agg_Users Table in DataBase
sql="""CREATE TABLE agg_users
(
State varChar(100),
Year INT,
Quater INT,
RegisteredUsers BIGINT,
AppOpens BIGINT,
Brand varChar(100),
BrandCount DECIMAL(20,2),
BrandPercentage DECIMAL(10,4)
);"""
curr.execute(sql)

#insert query
insert="""INSERT INTO agg_users
(State, Year, Quater, RegisteredUsers, AppOpens, Brand, BrandCount, BrandPercentage)
 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
#convert DataFrame rows into a list of tuples
rows=Agg_Users.values.tolist()
curr.executemany(insert,rows)
db_Connection.commit()

#SQL query to create Map_Transaction Table in DataBase
sql="""CREATE TABLE map_transaction
(
State varChar(100),
Year INT,
Quater INT,
StateDistrict varChar(100),
Transaction_count BIGINT,
Transaction_amount DECIMAL(20,6)
);"""
curr.execute(sql)

#insert query
insert="""INSERT INTO map_transaction
(State, Year, Quater, StateDistrict, Transaction_count, Transaction_amount)
 VALUES (%s, %s, %s, %s, %s, %s)"""
#convert DataFrame rows into a list of tuples
rows=Map_Transaction.values.tolist()
curr.executemany(insert,rows)
db_Connection.commit()

#SQL query to create Map_Insurance Table in DataBase
sql="""CREATE TABLE map_insurance
(
State varChar(100),
Year INT,
Quater INT,
StateDistrict varChar(100),
Transaction_count BIGINT,
Transaction_amount DECIMAL(20,6)
);"""
curr.execute(sql)

#insert query
insert="""INSERT INTO map_insurance
(State, Year, Quater, StateDistrict, Transaction_count, Transaction_amount)
 VALUES (%s, %s, %s, %s, %s, %s)"""
#convert DataFrame rows into a list of tuples
rows=Map_Insurance.values.tolist()
curr.executemany(insert,rows)
db_Connection.commit()

#SQL query to create Map_Users Table in DataBase
sql="""CREATE TABLE map_users
(
State varChar(100),
Year INT,
Quater INT,
StateDistrict varChar(100),
RegisteredUsers BIGINT,
AppOpens BIGINT
);"""
curr.execute(sql)

#insert query
insert="""INSERT INTO map_users
(State, Year, Quater, StateDistrict, RegisteredUsers, AppOpens)
 VALUES (%s, %s, %s, %s, %s, %s)"""
#convert DataFrame rows into a list of tuples
rows=Map_Users.values.tolist()
curr.executemany(insert,rows)
db_Connection.commit()

#SQL query to create Top_Transaction Table in DataBase
sql="""CREATE TABLE top_transaction
(
State varChar(100),
Year INT,
Quater INT,
EntityLevel varChar(100),
EntityValue varChar(100),
Transaction_count BIGINT,
Transaction_amount DECIMAL(20,6)
);"""
curr.execute(sql)

#insert query
insert="""INSERT INTO top_transaction
(State, Year, Quater, EntityLevel, EntityValue, Transaction_count, Transaction_amount)
 VALUES (%s, %s, %s, %s, %s, %s, %s)"""
#convert DataFrame rows into a list of tuples
rows=Top_Transaction.values.tolist()
curr.executemany(insert,rows)
db_Connection.commit()

#SQL query to create Top_Insurance Table in DataBase
sql="""CREATE TABLE top_insurance
(
State varChar(100),
Year INT,
Quater INT,
EntityLevel varChar(100),
EntityValue varChar(100),
Transaction_count BIGINT,
Transaction_amount DECIMAL(20,6)
);"""
curr.execute(sql)

#insert query
insert="""INSERT INTO top_insurance
(State, Year, Quater, EntityLevel, EntityValue, Transaction_count, Transaction_amount)
 VALUES (%s, %s, %s, %s, %s, %s, %s)"""
#convert DataFrame rows into a list of tuples
rows=Top_Insurance.values.tolist()
curr.executemany(insert,rows)
db_Connection.commit()

#SQL query to create Top_User Table in DataBase
sql="""CREATE TABLE top_user
(
State varChar(100),
Year INT,
Quater INT,
EntityLevel varChar(100),
EntityValue varChar(100),
RegisteredUsers BIGINT
);"""
curr.execute(sql)

#insert query
insert="""INSERT INTO top_user
(State, Year, Quater, EntityLevel, EntityValue, RegisteredUsers)
 VALUES (%s, %s, %s, %s, %s, %s)"""
#convert DataFrame rows into a list of tuples
rows=Top_User.values.tolist()
curr.executemany(insert,rows)
db_Connection.commit()

curr.close()
db_Connection.close()

