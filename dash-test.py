import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
from datetime import datetime as dt


app = dash.Dash()
app.layout = html.Div(children=[
    html.H1("MVP - Time to Upgrade"),
    html.Div(children='''
    symbol to graph:'''),
    dcc.Input(id='input', value='', type='text'),
    html.Div(id='output-graph'),]
)

@app.callback(
    Output(component_id='output-graph',component_property='children'),
    [Input(component_id='input',component_property='value')]
    )
def update_graph(input_data):
    path = "./daily-historical-stock-prices-1970-2018/"

    df_price = pd.read_csv(path + 'historical_stock_prices.csv', nrows=200)
    # df_stock = pd.read_csv(path + 'historical_stocks.csv')
    selected_ticker = input_data
    selected_col1 = 'open'
    selected_col2 = 'close'

    list_open = df_price[selected_col1][df_price['ticker'] == selected_ticker]
    list_close = df_price[selected_col2][df_price['ticker'] == selected_ticker]
    list_date = df_price['date'][df_price['ticker'] == selected_ticker]

    return dcc.Graph(id='example',
              figure={
                  'data':[{'x':list_date,
                           'y':list_open,
                           'type':'line',
                           'name': selected_col1},
                          {'x': list_date,
                           'y': list_close,
                           'type': 'bar',
                           'name': selected_col2}
                          ],
                  'layout':{
                      'title': f'{selected_ticker} historical data'
                  }

    }),



def s_buy_hold(stocks, df_price, start_date='2014-05-08', end_date='2014-05-14', invest=100, frequency=30):
    # strategy 1. buy invest amount at start_date, calculate the PnL at end_date
    pnl = 0
    if dt.strptime(end_date, "%Y-%m-%d") <= dt.strptime(start_date, "%Y-%m-%d"):
        return 'You end date is before your start date!'
    if len(stocks) == 0:
        return 'You stock ticker is out of scope.'
    for stock in stocks:
        select = df_price[df_price['ticker'] == stock][['date', 'adj_close']].copy()
        if start_date in select['date'].values:
            p0=select['adj_close'][select['date']==start_date].values[0]
            if p0 == 0:
                return "Starting price is zero."
            else:
                q = invest/p0
        else:
            return 'start date or ticker doesn\'t exist'
        if end_date in select['date'].values:
            p1=select['adj_close'][select['date']==end_date].values[0]
            pnl = q * (p1 - p0)
        else:
            return 'end date out of scope.'
    return pnl

if __name__ == '__main__':
    app.run_server(debug=True)