# Imports
#py -m pip install pandas_datareader
from audioop import avg
from pandas_datareader import data as pdr
from yahoo_fin import stock_info as si
from pandas import ExcelWriter
import yfinance as yf
import pandas as pd
import datetime
import time
import os
import glob

if os.path.exists("ScreenOutput.xlsx"):
    os.remove("ScreenOutput.xlsx")

f = open("watchlist_python", "w")
f.write("COLUMN,0\n")
f.write("HED,Python WhatchList\n")
f.close()

f = open("watchlist_python_near_52", "w")
f.write("COLUMN,0\n")
f.write("HED,Python WhatchList\n")
f.close()

f = open("watchlist_python_big_volume", "w")
f.write("COLUMN,0\n")
f.write("HED,Python WhatchList\n")
f.close()

f = open("watchlist_python_new_max", "w")
f.write("COLUMN,0\n")
f.write("HED,Python WhatchList\n")
f.close()

yf.pdr_override()

# Variables
tickers = si.tickers_sp500()
tickers = [item.replace(".", "-") for item in tickers] # Yahoo Finance uses dashes instead of dots
#tickers = ['A', 'AAL', 'AAPL', 'SLB', 'CTVA']
#tickers = ['CTVA']
index_name = '^GSPC' # S&P 500
start_date = datetime.datetime.now() - datetime.timedelta(days=365)
end_date = datetime.date.today()
exportList = pd.DataFrame(columns=['Stock', "RS_Rating", "50 Day MA", "150 Day Ma", "200 Day MA", "52 Week Low", "52 week High"])
returns_multiples = []

# Index Returns
index_df = pdr.get_data_yahoo(index_name, start_date, end_date)
index_df['Percent Change'] = index_df['Adj Close'].pct_change()
index_return = (index_df['Percent Change'] + 1).cumprod()[-1]

# Find top 30% performing stocks (relative to the S&P 500)
for ticker in tickers:
    # Download historical data as CSV for each stock (makes the process faster)
    print('starting with ticker: {ticker}')
    df = pdr.get_data_yahoo(ticker, start_date, end_date)
    df.to_csv(f'{ticker}.csv')
    

    # Calculating returns relative to the market (returns multiple)
    df['Percent Change'] = df['Adj Close'].pct_change()
    stock_return = (df['Percent Change'] + 1).cumprod()[-1]
    
    returns_multiple = round((stock_return / index_return), 2)
    returns_multiples.extend([returns_multiple])
    
    print (f'Ticker: {ticker}; Returns Multiple against S&P 500: {returns_multiple}\n')
    time.sleep(1)

# Creating dataframe of only top 30%
rs_df = pd.DataFrame(list(zip(tickers, returns_multiples)), columns=['Ticker', 'Returns_multiple'])
rs_df['RS_Rating'] = rs_df.Returns_multiple.rank(pct=True) * 100
rs_df = rs_df[rs_df.RS_Rating >= rs_df.RS_Rating.quantile(.70)]

# Checking Minervini conditions of top 30% of stocks in given list
rs_stocks = rs_df['Ticker']
for stock in rs_stocks:    
    try:
        df = pd.read_csv(f'{stock}.csv', index_col=0)
        sma = [50, 150, 200]
        for x in sma:
            df["SMA_"+str(x)] = round(df['Adj Close'].rolling(window=x).mean(), 2)
        
        df["AVGVOL"] = round(df['Volume'].rolling(window=50).mean(), 2)
        # Storing required values 
        currentClose = df["Adj Close"][-1]
        lastDayVolume = df["Volume"][-1]
        moving_average_50 = df["SMA_50"][-1]
        moving_average_150 = df["SMA_150"][-1]
        moving_average_200 = df["SMA_200"][-1]
        avg_vol = df["AVGVOL"][-1]
        low_of_52week = round(min(df["Low"][-260:]), 2)
        high_of_52week = round(max(df["High"][-260:]), 2)
        low_of_21day = round(min(df["Low"][-21:-4]), 2)
        high_of_21day = round(max(df["High"][-21:-4]), 2)
        low_of_3day = round(min(df["Low"][-3:]), 2)
        high_of_3day = round(max(df["High"][-3:]), 2)
        


        RS_Rating = round(rs_df[rs_df['Ticker']==stock].RS_Rating.tolist()[0])
        
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
        
        # Condition 8: 21 days volatility < 25%
        condition_8 = ((high_of_21day-low_of_21day)/high_of_21day)*100 < 25

        # Condition 9: 3 days volatility < 5%
        condition_9 = ((high_of_3day-low_of_3day)/high_of_3day)*100 < 5

        # Condition 10: 4% hasta maximos de 52 w
        condition_10 = ((high_of_52week - currentClose) > 0) and (((high_of_52week-currentClose)/currentClose)*100 < 4)

        # Condition 11: Extra volume
        condition_11 = lastDayVolume > (2 * avg_vol)

        # Condition 12: Current Close > 52 week close
        condition_12 = currentClose >= high_of_52week

        # If all conditions above are true, add stock to exportList
        if(condition_0 and condition_1 and condition_2 and condition_3 and condition_4 and condition_5 and condition_6 and condition_7 and condition_8 and condition_9):
            exportList = exportList.append({'Stock': stock, "RS_Rating": RS_Rating ,"50 Day MA": moving_average_50, "150 Day Ma": moving_average_150, "200 Day MA": moving_average_200, "52 Week Low": low_of_52week, "52 week High": high_of_52week}, ignore_index=True)
            print (stock + " made the Minervini requirements")
            f = open("watchlist_python", "a")
            f.write(f"SYM,{stock},SMART/AMEX,\n")
            f.close()

        if (condition_0 and condition_10):
            f = open("watchlist_python_near_52", "a")
            f.write(f"SYM,{stock},SMART/AMEX,\n")
            f.close()

        if (condition_0 and condition_11):
            f = open("watchlist_python_big_volume", "a")
            f.write(f"SYM,{stock},SMART/AMEX,\n")
            f.close()
        
        if (condition_0 and condition_12):
            f = open("watchlist_python_new_max", "a")
            f.write(f"SYM,{stock},SMART/AMEX,\n")
            f.close()

    except Exception as e:
        print (e)
        print(f"Could not gather data on {stock}")

exportList = exportList.sort_values(by='RS_Rating', ascending=False)
print('\n', exportList)
writer = ExcelWriter("ScreenOutput.xlsx")
exportList.to_excel(writer, "Sheet1")
writer.save()

fileList = glob.glob("*.csv")
for filePath in fileList:
    try:
        os.remove(filePath)
        #print(filePath)
        #print("\n")
    except:
        print("Error while deleting file : ", filePath)

