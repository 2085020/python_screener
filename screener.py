# Imports

#Pending
#Volumen de los últimos 10-20 días de los que sobrepasan la VMA50. Siendo el volumen de la suma de todos estos superior a un 200% de la VMA50
#Acción dentro de un rango del <20% durante los últimos 10 días
#Acción no más lejos del 20% de la MA50
#Últimos 3 días en un rango del 8%

#py -m pip install pandas_datareader
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

#https://www.nasdaq.com/market-activity/stocks/screener?exchange=NASDAQ&render=download
#https://www.nasdaq.com/market-activity/stocks/screener?exchange=NYSE&render=download

descarga = False
borrado = True

if os.path.exists("ScreenOutput.xlsx"):
    os.remove("ScreenOutput.xlsx")

if os.path.exists("ScreenOutput2.xlsx"):
    os.remove("ScreenOutput2.xlsx")

f = open("watchlist_python_minervini_earnings", "w")
f.write("COLUMN,0\n")
f.write("HED,Minervini WhatchList Earnings\n")
f.close()

f = open("watchlist_python_minervini", "w")
f.write("COLUMN,0\n")
f.write("HED,Minervini WhatchList\n")
f.close()

f = open("watchlist_python_near_52", "w")
f.write("COLUMN,0\n")
f.write("HED,Near High WhatchList\n")
f.close()

f = open("watchlist_python_big_volume", "w")
f.write("COLUMN,0\n")
f.write("HED,Big Volume WhatchList\n")
f.close()

f = open("watchlist_python_new_max", "w")
f.write("COLUMN,0\n")
f.write("HED,New Max WhatchList\n")
f.close()

f = open("watchlist_python_bullish_inside", "w")
f.write("COLUMN,0\n")
f.write("HED,Bullish Inside WhatchList\n")
f.close()

f = open("watchlist_python_bearish_inside", "w")
f.write("COLUMN,0\n")
f.write("HED,Bearish Inside WhatchList\n")
f.close()

f = open("watchlist_python_bullish_engulfing", "w")
f.write("COLUMN,0\n")
f.write("HED,Bullish Eng WhatchList\n")
f.close()

f = open("watchlist_python_bearish_engulfing", "w")
f.write("COLUMN,0\n")
f.write("HED,Bear Eng WhatchList\n")
f.close()


yf.pdr_override()

# Variables
#tickers = si.tickers_sp500()
#tickers = [item.replace(".", "-") for item in tickers] # Yahoo Finance uses dashes instead of dots
# Variables
# NYSE & NASDAQ

csv_path = "nasdaq_screener.csv"
df_stocks = pd.read_csv(csv_path)
df_stocks = df_stocks[df_stocks.MarketCap >= 400000000]
df_stocks = df_stocks[df_stocks.Volume > 200000]
tickers = df_stocks['Symbol']
"""
#tickers = ['A', 'AAL', 'AAPL', 'SLB', 'CTVA', 'BMY', 'XOM']
#tickers = ['BMY']
#tickers = ['ACC','AEE','AEP','AMCR','AMGN','AMX','APA','APTS','AR','ARLP','ASZ','ATRS','BMY','CDK','CERN','CHK','CHNG','CI','CIVI','CMS','CNC','CNR','COP','CPG','CRHC','CTRA','CTVA','CVBF','CVE','CVX','DINO','DOX','DUK','DVN','ED','ELP','EOG','EPD','EQT','ES','EXC','FTS','GO','HES','HOLX','HRB','IMO','JNJ','LLY','MCK','MPC','MRK','MRO','MSP','MTOR','NJR','NRG','NTCT','NTUS','NVS','ORAN','POST','PPC','PPL','PSX','PXD','SAIL','SBLK','SD','SFL','SHEL','SJI','SNY','SO','SQM','SRRA','STNG','SU','SWCH','SWX','TEF','TRP','TS','TVTY','WEC','WMB','XEL','XOM']
"""

start_date = datetime.datetime.now() - datetime.timedelta(days=365)
end_date = datetime.date.today()
exportList = pd.DataFrame(columns=['Stock', "RS_Rating", "50 Day MA", "150 Day Ma", "200 Day MA", "52 Week Low", "52 week High"])
exportList2 = pd.DataFrame(columns=['Stock', "RS_Rating", "50 Day MA", "150 Day Ma", "200 Day MA", "52 Week Low", "52 week High"])
returns_multiples = []

# Index Returns
# ALL INDEXES
index_name = '^GSPC' # S&P 500
index_sp = pdr.get_data_yahoo(index_name, start_date, end_date)
index_name = '^NYA' # NYSE
index_nya = pdr.get_data_yahoo(index_name, start_date, end_date)
index_name = '^IXIC' # NASDAQ COMPOSITE
index_ixic = pdr.get_data_yahoo(index_name, start_date, end_date)
index_df = index_nya + index_ixic + index_sp

index_df['Percent Change'] = index_df['Adj Close'].pct_change()
index_return = (index_df['Percent Change'] + 1).cumprod()[-1]

# Find top 30% performing stocks (relative to the S&P 500)
for ticker in tickers:
    # Download historical data as CSV for each stock (makes the process faster)
    print(f'starting with ticker: {ticker}')
    if not descarga and os.path.exists(f'{ticker}.csv'):
        df = pd.read_csv(f'{ticker}.csv', index_col=0)      
        print('Read CSV: {ticker}')  
    else:
        df = pdr.get_data_yahoo(ticker, start_date, end_date)
        df.to_csv(f'{ticker}.csv')
        print('Yahhoo CSV: {ticker}')  
    

    # Calculating returns relative to the market (returns multiple)
    df['Percent Change'] = df['Adj Close'].pct_change()
    stock_return = (df['Percent Change'] + 1).cumprod()[-1]
    stock_return_ad = float(stock_return)

    if math.isnan(stock_return_ad):
        tickers.remove(ticker)
        print("Ticker Removed: " + ticker)
    else: 
        returns_multiple = round((stock_return / index_return), 2)
        returns_multiples.extend([returns_multiple])
    
    print (f'Ticker: {ticker}; Returns Multiple against S&P 500: {returns_multiple}\n')
    time.sleep(0.3)

# Creating dataframe of only top 20%
valueRating = 0.8
#Creating dataframe of only top 30%
valueRating = 0.7
rs_df = pd.DataFrame(list(zip(tickers, returns_multiples)), columns=['Ticker', 'Returns_multiple'])
rs_df['RS_Rating'] = rs_df.Returns_multiple.rank(pct=True) * 100
rs_df = rs_df[rs_df.RS_Rating >= rs_df.RS_Rating.quantile(valueRating)]

# Checking Minervini conditions of top 30% of stocks in given list
rs_stocks = rs_df['Ticker']
for stock in rs_stocks:    
    try:
        df = pd.read_csv(f'{stock}.csv', index_col=0)
        sma = [5, 10, 20, 30, 50, 150, 200]
        for x in sma:
            df["SMA_"+str(x)] = round(df['Adj Close'].rolling(window=x).mean(), 2)
        
        df["AVGVOL"] = round(df['Volume'].rolling(window=50).mean(), 2)
        # Storing required values 
        currentClose = df["Adj Close"][-1]
        lastDayVolume = df["Volume"][-1]
        moving_average_50 = df["SMA_50"][-1]
        moving_average_150 = df["SMA_150"][-1]
        moving_average_200 = df["SMA_200"][-1]
        moving_average_5 = df["SMA_5"][-1]
        moving_average_10 = df["SMA_10"][-1]
        moving_average_30 = df["SMA_30"][-1]
        moving_average_20 = df["SMA_20"][-1]
        avg_vol = df["AVGVOL"][-1]
        low_of_52week = round(min(df["Low"][-260:]), 2)
        high_of_52week = round(max(df["High"][-260:]), 2)
        low_of_21day = round(min(df["Low"][-21:-4]), 2)
        high_of_21day = round(max(df["High"][-21:-4]), 2)
        low_of_3day = round(min(df["Low"][-3:]), 2)
        high_of_3day = round(max(df["High"][-3:]), 2)

        high_of_2day = round(df["High"][-2], 2)
        low_of_2day = round(df["Low"][-2], 2)
        high_of_1day = round(df["High"][-1], 2)
        low_of_1day = round(df["Low"][-1], 2)
        lastOpen = round(df["Open"][-1], 2)
        lastClose = round(df["Close"][-1], 2)
        
        stDev = df["Adj Close"][-21:].std()


        RS_Rating = round(rs_df[rs_df['Ticker']==stock].RS_Rating.tolist()[0])
        
        try:
            moving_average_200_20 = df["SMA_200"][-20]
        except Exception:
            moving_average_200_20 = 0

        # Condition 0: Average volume > 200.000
        condition_0 = avg_vol > 200000

        # Condition 0: Average volume > 500.000
        condition_0b = avg_vol > 500000

        # Condition 1: Current Price > 150 SMA and > 200 SMA
        condition_1 = currentClose > moving_average_150 > moving_average_200
        
        # Condition 2: 150 SMA and > 200 SMA
        condition_2 = moving_average_150 > moving_average_200

        # Condition 3: 200 SMA trending up for at least 1 month
        condition_3 = moving_average_200 > moving_average_200_20
        
        # Condition 4: 50 SMA> 150 SMA and 50 SMA> 200 SMA
        condition_4 = moving_average_50 > moving_average_150 > moving_average_200
           
        # Condition 5: Current Price > 50 SMA
        condition_5 = currentClose > moving_average_50
           
        # Condition 6: Current Price is at least 30% above 52 week low
        condition_6 = currentClose >= (1.3*low_of_52week)
           
        # Condition 7: Current Price is within 25% of 52 week high
        condition_7 = currentClose >= (.75*high_of_52week)
        
        # Condition 8: 21 days volatility < 25%
        condition_8 = ((high_of_21day-low_of_21day)/high_of_21day)*100 < 25

        # Condition 9: 3 days volatility < 5%
        condition_9 = ((high_of_3day-low_of_3day)/high_of_3day)*100 < 10

        # Condition 10: más de un 3% hasta maximos de 52 w
        condition_10 = ((high_of_52week - currentClose) > 0) and (((high_of_52week-currentClose)/high_of_52week)*100 > 3)

        # Condition 10: 8% hasta maximos de 52 w
        condition_10b = ((high_of_52week - currentClose) > 0) and (((high_of_52week-currentClose)/currentClose)*100 < 8)
        
        # Condition 11: Current Price not far than a 15% from 50 SMA
        condition_11 = currentClose < moving_average_50*1.15

        # Condition 12: Extra volume
        condition_12 = lastDayVolume > (2 * avg_vol)

        # Condition 13: Current Close > 52 week close
        condition_13 = currentClose >= high_of_52week

        #Condition 14: Bullish inside day
        Condition_14BuI = high_of_2day > high_of_1day and low_of_2day < low_of_1day and lastOpen < lastClose

        #Condition 14: Bearish inside day
        Condition_14BeI = high_of_2day > high_of_1day and low_of_2day < low_of_1day and lastOpen > lastClose

        #Condition 14: Bullish engulfing 
        Condition_14BuE = high_of_2day < lastClose and low_of_2day > lastOpen

        #Condition 14: Bearish engulfing
        Condition_14BeE = high_of_2day < lastOpen and low_of_2day > lastClose

        # Condition 13: 3% hasta la media de 20
        #condition_13 = (abs(currentClose - moving_average_20)/currentClose)*100 < 3

        # Condition 14: 5% hasta la media de 30
        #condition_14 = (abs(currentClose - moving_average_30)/currentClose)*100 < 5

        # If all conditions above are true, add stock to exportList
        if(condition_0b and condition_1 and condition_2 and condition_3 and condition_4 and condition_5 and condition_6 and condition_7 and condition_8 and condition_9 and condition_10 and condition_10b and condition_11):
        #if(condition_0 and condition_1 and condition_2 and condition_3 and condition_4 and condition_5 and condition_6 and condition_7 and condition_8 and condition_9 and condition_13):
            exportList2 = exportList2.append({'Stock': stock, "RS_Rating": RS_Rating ,"50 Day MA": moving_average_50, "150 Day Ma": moving_average_150, "200 Day MA": moving_average_200, "52 Week Low": low_of_52week, "52 week High": high_of_52week}, ignore_index=True)
            
            print("Minervini for stock" + stock)
            print("calculate EPS:"+stock)
            epsGrowth = calculateEPS(stock)
            print("calculate Sales:"+stock)
            salesGrowth = calculateRevenueGrowth(stock)
            print("End calculate:"+stock)
                
            if (epsGrowth > 10 and salesGrowth > 10):
                exportList = exportList.append({'Stock': stock, "RS_Rating": RS_Rating ,"50 Day MA": moving_average_50, "150 Day Ma": moving_average_150, "200 Day MA": moving_average_200, "52 Week Low": low_of_52week, "52 week High": high_of_52week}, ignore_index=True)
                print (stock + " made the Minervini requirements + earnings")
                f = open("watchlist_python_minervini_earnings", "a")
                f.write(f"SYM,{stock},SMART/AMEX,\n")
                f.close()
            else:
                print (stock + " made the Minervini requirements")
                f = open("watchlist_python_minervini", "a")
                f.write(f"SYM,{stock},SMART/AMEX,\n")
                f.close()

        if (condition_0b and condition_10b):
            f = open("watchlist_python_near_52", "a")
            f.write(f"SYM,{stock},SMART/AMEX,\n")
            f.close()

        if (condition_0 and condition_12):
            f = open("watchlist_python_big_volume", "a")
            f.write(f"SYM,{stock},SMART/AMEX,\n")
            f.close()
        
        if (condition_0 and condition_13):
            f = open("watchlist_python_new_max", "a")
            f.write(f"SYM,{stock},SMART/AMEX,\n")
            f.close()

        if (condition_0b and Condition_14BuI):
            f = open("watchlist_python_bullish_inside", "a")
            f.write(f"SYM,{stock},SMART/AMEX,\n")
            f.close()

        if (condition_0b and Condition_14BeI):
            f = open("watchlist_python_bearish_inside", "a")
            f.write(f"SYM,{stock},SMART/AMEX,\n")
            f.close()
        
        if (condition_0b and Condition_14BuE):
            f = open("watchlist_python_bullish_engulfing", "a")
            f.write(f"SYM,{stock},SMART/AMEX,\n")
            f.close()

        if (condition_0b and Condition_14BeE):
            f = open("watchlist_python_bearish_engulfing", "a")
            f.write(f"SYM,{stock},SMART/AMEX,\n")
            f.close()


    except Exception as e:
        print (e)
        print(f"Could not gather data on {stock}")

exportList = exportList.sort_values(by='RS_Rating', ascending=False)
print('\n', exportList)

exportList2 = exportList2.sort_values(by='RS_Rating', ascending=False)
print('\n', exportList2)
writer = ExcelWriter("ScreenOutput.xlsx")
exportList.to_excel(writer, "Sheet1")
writer.save()

writer2 = ExcelWriter("ScreenOutput2.xlsx")
exportList2.to_excel(writer2, "Sheet1")
writer2.save()

if borrado:
    fileList = glob.glob("*.csv")
    for filePath in fileList:
        try:
            if (filePath.find("screener")==-1):
                os.remove(filePath)
            #print(filePath)
            #print("\n")
        except:
            print("Error while deleting file : ", filePath)

