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
    val = (actStock.Symbol, actStock.Name, actStock.MarketCap, actStock.Country, actStock.Sector, actStock.Industry)
    mycursor.execute(sql, val)