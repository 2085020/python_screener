"""
from pandas_datareader import data as pdr
from yahoo_fin import stock_info as si
from pandas import ExcelWriter
import yfinance as yf
import pandas as pd
import datetime
import time
import os
import glob
import math
from functions import *
import mysql.connector

csv_path = "nasdaq_screener.csv"
df_stocks = pd.read_csv(csv_path)
df_stocks = df_stocks[df_stocks.MarketCap >= 400000000]
df_stocks = df_stocks[df_stocks.Volume > 200000]
myvalue = df_stocks[df_stocks.Symbol == 'AAPL']
tickers = df_stocks['Symbol']


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="UsedPass",
  database="stocks"
)

mycursor = mydb.cursor()
sql = "INSERT INTO stock_data (Symbol, Name, MarketCap, Country, Sector, Industry) VALUES (%s, %s, %s, %s, %s, %s)"


for ticker in tickers:
    print(ticker)
    actStock = df_stocks[df_stocks.Symbol == ticker]
    val = (str(actStock.Symbol), actStock.Name, actStock.MarketCap, actStock.Country, actStock.Sector, actStock.Industry)
    #mycursor.execute(sql, val)
"""
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector

# Credentials to database connection
hostname="localhost"
dbname="stocks"
uname="root"
pwd="USERPASS"

mydb = mysql.connector.connect(
  host=hostname,
  user=uname,
  password=pwd,
  database=dbname
)

mycursor = mydb.cursor()

sql = "DROP TABLE stock_data2"

mycursor.execute(sql)

# Create dataframe
csv_path = "nasdaq_screener.csv"
df_stocks = pd.read_csv(csv_path)
df_stocks = df_stocks[df_stocks.MarketCap >= 400000000]
df = df_stocks[df_stocks.Volume > 200000]

# Create SQLAlchemy engine to connect to MySQL Database
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
				.format(host=hostname, db=dbname, user=uname, pw=pwd))

# Convert dataframe to sql table                                   
df.to_sql('stock_data2', engine, index=False)
