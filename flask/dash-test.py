import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
from datetime import datetime as dt
import plotly.graph_objs as go
from collections import deque


app = dash.Dash()
path = "./daily-historical-stock-prices-1970-2018/"
df_price = pd.read_csv(path + 'historical_stock_prices.csv', nrows=2000)

# dropdown options
features = df_price.columns[1:-1]
opts = [{'label': i, 'value': i} for i in features]

# range slider options
# df_price['date'] = pd.to_datetime(df_price.date)
# dates = ['2011-02-17', '2011-05-17', '2011-08-17', '2011-11-17',
#          '2014-02-17', '2014-05-17', '2014-08-17', '2014-11-17', '2015-02-17']


app.layout = html.Div(
    children=[
        html.Div([
            html.H1("Trading Strategy Testing Dashboard"),
            html.P("MVP testing version 0.1"),],
            style={'padding': '20px',
                   'backgroundColor': '#08BB98'}),
        html.Div(children='''
        Symbol to graph:'''),
        dcc.Input(id='input', value='', type='text'),
        html.Div(children='''
        Start Date:'''),
        dcc.Input(id='input-start', value='', type='text'),
        html.Div(children='''
        End Date:'''),
        dcc.Input(id='input-end', value='', type='text'),

        # dropdown
        # html.P([
        #     html.Label("Choose a feature"),
        #     dcc.Dropdown(id='opt', options=opts,
        #                  value=opts[0])
        # ], style={'width': '400px',
        #           'fontSize': '20px',
        #           'padding-left': '100px',
        #           'display': 'inline-block'}),

        html.Div(id='output-graph'),
        html.Div(children='''
        PnL for your trade:'''),
        html.Div(id='output-pnl'),]
)


@app.callback(
    Output(component_id='output-pnl',component_property='children'),
    [Input(component_id='input',component_property='value'),
     Input(component_id='input-start',component_property='value'),
     Input(component_id='input-end',component_property='value'),]
    )


def update_pnl(input_data, start_date, end_date):
    path = "./daily-historical-stock-prices-1970-2018/"
    df_price = pd.read_csv(path + 'historical_stock_prices.csv', nrows=2000)
    if start_date and end_date:
        pnl = s_buy_hold(input_data.upper(), df_price, start_date, end_date)
        return f'{pnl:.2f}'
    else:
        return "Not enough input"


@app.callback(
    Output(component_id='output-graph',component_property='children'),
    [Input(component_id='input',component_property='value')]
    )
def update_graph(input_data):
    # df_stock = pd.read_csv(path + 'historical_stocks.csv')
    selected_ticker = input_data.upper()
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


def s_buy_hold(stocks, df_price, start_date='2013-05-08', end_date='2013-05-14', invest=100, frequency=30):
    # strategy 1. buy invest amount at start_date, calculate the PnL at end_date
    pnl = 0
    if dt.strptime(end_date, "%Y-%m-%d") <= dt.strptime(start_date, "%Y-%m-%d"):
        return 'You end date is before your start date!'
    if len(stocks) == 0:
        return 'You stock ticker is out of scope.'

    if stocks not in df_price['ticker'].values:
        return 'stock doesn\'t exist'
    else:
        select = df_price[df_price['ticker'] == stocks][['date', 'adj_close']].copy()

    if start_date in select['date'].values:
        p0=select['adj_close'][select['date']==start_date].values[0]
        if p0 == 0:
            return "Starting price is zero."
        else:
            q = invest/p0
    else:
        return 'start date doesn\'t exist'
    if end_date in select['date'].values:
        p1=select['adj_close'][select['date']==end_date].values[0]
        pnl = q * (p1 - p0)
    else:
        return 'end date out of scope.'
    return pnl


if __name__ == '__main__':
    app.run_server(debug=True)