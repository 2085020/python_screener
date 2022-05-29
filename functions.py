from yahoo_fin import stock_info as si

#Calculate EPS
def calculateEPS(ticker):
    actualEps = 0
    lastEps = 0
    epsGrowth = 0
    earnings = si.get_earnings_history(ticker)
    for row in earnings:
        if row["epsactual"] != None and row['epsactual'] != '-' and lastEps == 0:
            if actualEps == 0:
                actualEps = row['epsactual']
            else:
                lastEps = row['epsactual']
            
            if lastEps != 0:
                epsGrowth = round(((actualEps - lastEps) / lastEps) * 100)
                break
    
    return epsGrowth


#Calculate Revenue
def calculateRevenueGrowth(ticker):
    actualSales = 0
    lastSales = 0
    salesGrowth = 0
    try:
        sales = si.get_income_statement(ticker, False)
        for row in sales:
            if lastSales == 0:
                if actualSales == 0:
                    actualSales =sales[row].totalRevenue
                else:
                    lastSales = sales[row].totalRevenue
                
                if lastSales != 0:
                    salesGrowth = round(((actualSales - lastSales) / lastSales) * 100)
                    break
    except Exception:
        pass
    return salesGrowth
