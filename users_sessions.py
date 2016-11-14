#Airbnb
#Where will a new guest book their first travel experience?

#Author: Liam Culligan
#Date: Februrary 2016

#Create a MySQL database hosted locally

#Import required packages and functions
import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine
from sqlalchemy import Table, MetaData, schema, Column
from sqlalchemy import BigInteger, Float, String, Date, text
from sqlalchemy import PrimaryKeyConstraint, Index, ForeignKeyConstraint

#Read in Pandas DataFrames
train = pd.read_csv("train_users_2.csv", header = 0)
test = pd.read_csv("test_users.csv", header = 0)

#Add placeholder target variable column to test - not provided for Kaggle competitions
test['country_destination'] = np.nan

#Combine train and test
df = pd.concat((train, test)).reset_index(drop=True)

#Convert to DateTime
df['date_account_created'] = pd.to_datetime(df['date_account_created'], format = "%Y-%m-%d")

#Get column names
col_names = list(df.columns.values)

#Get Python data types
col_dtypes = df.dtypes.tolist()

#Install MySQL driver
pymysql.install_as_MySQLdb()

#Create engine
#dialect+driver://username:password@host:port/database
engine =  create_engine('mysql://root:@localhost')

#Create Database
engine.execute("CREATE DATABASE airbnb_kaggle") #create db
engine.execute("USE airbnb_kaggle") # select new db

#Create table
metadata = MetaData()

#Empty Table
users = Table("users", metadata)

#Function to convert Python data types to SQL data types
def sqlcol(col_type):    
    if col_type == "object": #If data type is object
        this_type = String(255)
    elif col_type == "int64": #If data type is int
        this_type = BigInteger
    elif col_type == "float64": #If data type is float
        this_type = Float(precision = 3)
    elif  col_type == "datetime64[ns]": #If data type is datetime
        this_type = Date 
    return this_type

#Create table with appropriate names and data types    
for i in range(0, len(col_names)):
    col = schema.Column(col_names[i], sqlcol(col_dtypes[i]))
    users.append_column(col)

#Add primary key
users.append_constraint(PrimaryKeyConstraint("id"))
#Add indices
users.append_constraint(Index("date_first_booking", text("date_first_booking")))
users.append_constraint(Index("date_account_created", text("date_account_created")))

#Create table train_users in MySQL database
users.create(engine)

#Insert DataFrame into table - could be more efficient
df.to_sql(con = engine, name='users', if_exists='append', index = False)

#Repeat similar procedure for sessions.csv

#Read in Pandas Data Frame
sessions = pd.read_csv("sessions.csv", header = 0)

#Get column names
col_names = list(sessions.columns.values)

#Get Python data types
col_dtypes = sessions.dtypes.tolist()

#Empty Table
sessions_table = Table("sessions", metadata)

#Add primary key
sessions_table.append_column(Column('row_id', BigInteger, primary_key = True))

#Create table with appropriate names and data types    
for i in range(0, len(col_names)):
    col = schema.Column(col_names[i], sqlcol(col_dtypes[i]))
    sessions_table.append_column(col)

#Add foreign key
sessions_table.append_constraint(ForeignKeyConstraint(["user_id"], ["users.id"], name = "fk"))

#Create table train_users in MySQL database
sessions_table.create(engine)

#Insert DataFrame into table
sessions.to_sql(con = engine, name='sessions', if_exists='append', index = False)
