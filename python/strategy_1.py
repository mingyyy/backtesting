import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import style
from datetime import datetime as dt

path = "./daily-historical-stock-prices-1970-2018/"
df_price = pd.read_csv(path + 'historical_stock_prices.csv', nrows=200, index_col=7, parse_dates=True)
df_stock = pd.read_csv(path + 'historical_stocks.csv')

style.use('ggplot')
start = dt(2011,1,1)
end = dt(2014,1,1)

# initialize list of lists
data = [['tom', 10], ['nick', 15], ['juli', 14]]

# Create the pandas DataFrame
df_buy = pd.DataFrame(data, columns=['date', 'price'])


def one_stock_once(stock, df, invest=100, threshold=10):
    # strategy 1. buy invest amount if lower than threshold, calculate the PnL at end of the period
    pnl = 0

    if len(stock) == 0:
        return 'You stock ticker is out of scope.'

    for d in df_price['date']:
        select = df_price[df_price['ticker'] == stock][['date', 'adj_close']].copy()
        if start_date in select['date'].values:
            p0=select['adj_close'][select['date'] == start_date].values[0]
            if p0 == 0:
                return "Starting price is zero."
            else:
                q = invest/p0
        else:
            return 'start date or ticker doesn\'t exist'
        if end_date in select['date'].values:
            p1=select['adj_close'][select['date'] == end_date].values[0]
            pnl = q * (p1 - p0)
        else:
            return 'end date out of scope.'
    return pnl
