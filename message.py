# Imports
from pandas_datareader import data as pdr
from yahoo_fin import stock_info as si
from pandas import ExcelWriter
import yfinance as yf
import pandas as pd
import datetime
import math
import time
import os
yf.pdr_override()

# Variables
# NYSE & NASDAQ
csv_path = "C:/Users/mesia/Desktop/Screeners/utils/nasdaq_screener_1653242945583.csv"
df_stocks = pd.read_csv(csv_path)
df_stocks = df_stocks[df_stocks.MarketCap >= 600000000]
#df_industries = df_stocks['Industry']
df_stocks = df_stocks['Symbol']
# TO DO iterar industrias (mirar si se pueden combinar e iterar las dos listas)

# Ticker Transformation
tickers = [item.replace(".", "-") for item in df_stocks] # Yahoo Finance uses dashes instead of dots

start_date = datetime.datetime.now() - datetime.timedelta(days=365)
end_date = datetime.date.today()
exportList = pd.DataFrame(columns=['Stock', "RS_Rating", "50 Day MA", "150 Day Ma", "200 Day MA", "52 Week Low", "52 week High"])
returns_multiples = []

# Index Returns
index_name = '^NYA' # NYSE
index_nya = pdr.get_data_yahoo(index_name, start_date, end_date)
index_name = '^IXIC' # NASDAQ COMPOSITE
index_ixic = pdr.get_data_yahoo(index_name, start_date, end_date)

index_df = index_nya + index_ixic # ALL INDEXES
index_df['Percent Change'] = index_df['Adj Close'].pct_change()
index_return = (index_df['Percent Change'] + 1).cumprod()[-1]

# DEMO - descarga = false. Evitamos la descarga de ficheros
#tickers = ['A', 'AAL', 'AAPL', 'SLB', 'CTVA', 'BMY']
descarga = True

# Find top 30% performing stocks (relative to the S&P 500)
for ticker in tickers:
    try:
        # Download historical data as CSV for each stock (makes the process faster)
        # adaptar iteración: ticker = ticker['Symbol']
        print('Starting with ticker: ' + ticker)
        if not descarga and os.path.exists(f'{ticker}.csv'):
            df = pd.read_csv(f'{ticker}.csv', index_col=0)      
            print('Read CSV: ' + ticker)  
        else:
            df = pdr.get_data_yahoo(ticker, start_date, end_date)
            df.to_csv(f'{ticker}.csv')
            print('Yahhoo CSV: '  + ticker)  

        # Calculating returns relative to the market (returns multiple)
        df['Percent Change'] = df['Adj Close'].pct_change()
        stock_return = (df['Percent Change'] + 1).cumprod()[-1]
        stock_return_ad = float(stock_return);

        if math.isnan(stock_return_ad):
            tickers.remove(ticker)
            print("Ticker Removed: " + ticker)
        else: 
            returns_multiple = round((stock_return / index_return), 2)
            returns_multiples.extend([returns_multiple])
    
        print (f'Ticker: {ticker}; Returns Multiple: {returns_multiple}\n')
        time.sleep(0.1)
    except Exception as e:
        print (e)
        print(f"Could not gather data on {ticker}")
        tickers.remove(ticker)


# Creating dataframe of only top 20%
rs_df = pd.DataFrame(list(zip(tickers, returns_multiples)), columns=['Ticker', 'Returns_multiple'])
rs_df['RS_Rating'] = rs_df.Returns_multiple.rank(pct=True) * 100
rs_df = rs_df[rs_df.RS_Rating >= rs_df.RS_Rating.quantile(.80)]

# Checking Minervini conditions of top 30% of stocks in given list
rs_stocks = rs_df['Ticker']
for stock in rs_stocks:    
    try:
        df = pd.read_csv(f'{stock}.csv', index_col=0)
        sma = [50, 150, 200]
        for x in sma:
            df["SMA_"+str(x)] = round(df['Adj Close'].rolling(window=x).mean(), 2)
        
        # TO DO es posible calcular el average volume de cada día??
        df["AVGVOL"] = round(df['Volume'].rolling(window=50).mean(), 2)
        
        # Storing required values 
        # TO DO que es el -1?
        currentClose = df["Adj Close"][-1]
        moving_average_50 = df["SMA_50"][-1]
        moving_average_150 = df["SMA_150"][-1]
        moving_average_200 = df["SMA_200"][-1]
        RS_Rating = round(rs_df[rs_df['Ticker']==stock].RS_Rating.tolist()[0])
        avg_vol = df["AVGVOL"][-1] 

        low_of_52week = round(min(df["Low"][-260:]), 2)
        high_of_52week = round(max(df["High"][-260:]), 2)
        low_of_21day = round(min(df["Low"][-21:]), 2)
        high_of_21day = round(max(df["High"][-21:]), 2)
        low_of_3day = round(min(df["Low"][-3:]), 2)
        high_of_3day = round(max(df["High"][-3:]), 2)
        
        try:
            moving_average_200_20 = df["SMA_200"][-20]
        except Exception:
            moving_average_200_20 = 0

        # Condition 0: Average volume > 500.000
        condition_0 = avg_vol > 500000

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
        
        # Condition 8: 21 days price under 25%
        condition_8 = ((high_of_21day-low_of_21day)/high_of_21day)*100 < 25

        # Condition 9: 3 days price under 10%
        condition_9 = ((high_of_3day-low_of_3day)/high_of_3day)*100 < 10

        # Condition 10: más de un 3% hasta maximos de 52 w
        condition_10 = ((high_of_52week - currentClose) > 0) and (((high_of_52week-currentClose)/high_of_52week)*100 > 3)

        # Condition 11: Current Price not far than a 15% from 50 SMA
        condition_11 = currentClose < moving_average_50*1.15
        

        # If all conditions above are true, add stock to exportList
        if(condition_0 and condition_1 and condition_2 and condition_3 and condition_4 and condition_5 and condition_6 and condition_7 and condition_8 and condition_9 and condition_10 and condition_11):
            exportList = exportList.append({'Stock': stock, "RS_Rating": RS_Rating ,"50 Day MA": moving_average_50, "150 Day Ma": moving_average_150, "200 Day MA": moving_average_200, "52 Week Low": low_of_52week, "52 week High": high_of_52week}, ignore_index=True)
            exportList2 = pd.concat([exportList, pd.DataFrame.from_records([{'Stock': stock, "RS_Rating": RS_Rating ,"50 Day MA": moving_average_50, "150 Day Ma": moving_average_150, "200 Day MA": moving_average_200, "52 Week Low": low_of_52week, "52 week High": high_of_52week}])])
            print (stock + " made the Minervini requirements")
    except Exception as e:
        print (e)
        print(f"Could not gather data on {stock}")

exportList = exportList.sort_values(by='RS_Rating', ascending=False)
print('\n', exportList)
out_path = "C:\\Users\\mesia\\Desktop\\Screeners\\ScreenOutput.xlsx"
writer = ExcelWriter(out_path)
exportList.to_excel(writer, "Sheet1")
writer.save()