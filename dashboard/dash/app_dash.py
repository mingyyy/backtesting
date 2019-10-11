import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go
from secrete import db_password, end_point
import psycopg2
from psycopg2.extensions import AsIs


# external JavaScript files
external_scripts = [
    {'src': 'https://cdn.polyfill.io/v2/polyfill.min.js'},
    {
        'src': 'https://cdnjs.cloudflare.com/ajax/libs/lodash.js/4.17.10/lodash.core.js',
        'integrity': 'sha256-Qqd/EfdABZUcAxjOkMi8eGEivtdTkh3b65xCZL4qAQA=',
        'crossorigin': 'anonymous'
    }
]

# external CSS stylesheets
external_stylesheets = [
    'https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css',
    {
        'href': 'https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css',
        'rel': 'stylesheet',
        'integrity': 'sha384-MCw98/SFnGE8fJT3GXwEOngsV7Zt27NXFoaoApmYm81iuXoPkFOJwJ8ERdknLPMO',
        'crossorigin': 'anonymous'
    }
]


app = dash.Dash(external_stylesheets=external_stylesheets,
                external_scripts=external_scripts)

# app.scripts.config.serve_locally = True
# app.css.config.serve_locally = True

host = end_point
tbl_name = 'test2'
tbl_name_ticker = 'test'
selected_col = 'purchase_price'
selected_sector = None
selected_ticker = None


def load_data(query, params):
    conn = psycopg2.connect(host=host, user='postgres', password=db_password)
    sql_command = (query)
    df = pd.read_sql(sql_command, conn, params=params)
    return df


def get_sector(tbl_name):
    q = "SELECT sector FROM %(tbl_name)s GROUP BY sector;"
    params = {'tbl_name': AsIs(tbl_name), 'sector': selected_sector, 'ticker': selected_ticker, 'col': AsIs(selected_col)}
    sector_list = load_data(q, params)['sector']

    opts_sector = [{'label': i, 'value': i} for i in sector_list]
    return opts_sector


def get_stock(tbl_name, sector):
    if len(sector) == 0:
        q = "SELECT ticker FROM %(tbl_name)s GROUP BY ticker;"
        params = {'tbl_name': AsIs(tbl_name), 'sector': None, 'ticker': None, 'col': AsIs(selected_col)}
    elif type(sector) is list and len(sector) != 1:
        q = "SELECT ticker FROM %(tbl_name)s GROUP BY sector, ticker HAVING sector IN %(sector)s;"
        params = {'tbl_name': AsIs(tbl_name), 'sector': AsIs(tuple(sector)), 'ticker': selected_ticker, 'col': AsIs(selected_col)}
    elif type(sector) is str:
        q = "SELECT ticker FROM %(tbl_name)s GROUP BY sector, ticker HAVING sector = %(sector)s;"
        params = {'tbl_name': AsIs(tbl_name), 'sector': sector, 'ticker': selected_ticker, 'col': AsIs(selected_col)}
    else:
        q = "SELECT ticker FROM %(tbl_name)s GROUP BY sector, ticker HAVING sector = %(sector)s;"
        params = {'tbl_name': AsIs(tbl_name), 'sector': sector[0], 'ticker': selected_ticker, 'col': AsIs(selected_col)}
    ticker_list = load_data(q, params)['ticker']
    opts = [{'label':  i, 'value': i} for i in ticker_list]
    return opts


# Define dropdown list.
opts_sector = get_sector(tbl_name)
first_sector = opts_sector[0]['value']

opts_ticker = get_stock(tbl_name, first_sector)
first_ticker = opts_ticker[0]['value']


# Define Range Slider options
q = "SELECT purchase_date FROM %(tbl_name)s GROUP BY purchase_date ORDER BY purchase_date;"
params = {'tbl_name': AsIs(tbl_name), 'sector': None, 'ticker': None, 'col': AsIs(selected_col)}
first_dates = load_data(q, params)
start_date = first_dates['purchase_date'].min()
end_date = first_dates['purchase_date'].max()


# Load Data from Postgres for graph
q = "SELECT purchase_date, %(col)s FROM %(tbl_name)s WHERE ticker=%(ticker)s ORDER BY purchase_date;"
params = {'tbl_name': AsIs(tbl_name_ticker), 'sector': None, 'ticker': first_ticker, 'col': AsIs(selected_col)}
df_init = load_data(q, params)


# define the number of marker for display
N = int(first_dates.count()[0])
step = 1
if N > 12:
    while int(N/step)>12:
        step += 1
else:
    step = N
date_list = first_dates['purchase_date'].tolist()
date_mark = {i: {'label': first_dates.iloc[i][0].strftime('%Y-%m'), 'style': {'transform': 'rotate(-60deg)'}}
             for i in range(0, N+step, step) if i < N}

# Initiate Graph
trace_1 = go.Scatter(x=df_init.purchase_date, y=df_init[selected_col],name=selected_col,
                    line=dict(width=2, color='rgb(229, 151, 50)'),mode='markers')
layout = go.Layout(title='Profit & Loss Plot', hovermode='closest')
fig = go.Figure(data=[trace_1], layout=layout)
fig.update_xaxes(title_text='Year')
fig.update_yaxes(title_text='Profit&Loss ($)')

# Create a Dash layout
app.layout = html.Div([
        # Header
        html.Div([
            html.H1("Trading Strategy Back Testing Dashboard"),
            html.H5("Final testing version 1.0"),],
            style={'paddingLeft': '10%',
                   'paddingBottom':'2.5%',
                   'paddingTop':'3%',
                   'backgroundColor': '#74F3FF'
                    }),

        # Dropdowns
        html.Div([
            # Sector
            html.Div(
            [
                html.H5('Select sectors'),
                dcc.Dropdown(id='opt_sector',
                             options=opts_sector,
                             value=first_sector,
                             placeholder="Select Sectors",
                             multi=True
                             ),
            ], style={'width': '30%',
                      'fontSize': '100%',
                      'paddingTop': '0%',
                      'paddingBottom':'0%',
                      'paddingLeft': '3%',
                      'display': 'inline-block'}),

            # Ticker based on Sector
            html.Div(


                [   html.H5('Select a ticker'),
                    dcc.Dropdown(id='opt_ticker',
                                 options=opts_ticker,
                                 value=first_ticker,
                                 placeholder="Choose a ticker",
                                 )
                ], style={'width': '30%',
                          'fontSize': '100%',
                          'paddingTop': '0%',
                          'paddingBottom':'0%',
                          'paddingLeft': '3%',
                          'display': 'inline-block'}),
        ],
            style={
            'borderBottom': 'thin lightgrey solid',
            'backgroundColor': 'rgb(250, 250, 250)',
            'padding': '1% 1%'
            }
        ),

        # Plot
        dcc.Graph(id='output-graph', figure=fig),

        # Range slider
        html.P([
            html.Label("Time Period"),
            dcc.RangeSlider(id='slider',
                            marks=date_mark,
                            min=0,
                            max=N+step,
                            value=[1, N+step]
                            )
                ], style={'width': '90%',
                          'fontSize': '100%',
                          'paddingLeft': '10%',
                          'display': 'inline-block',
                          })
    ])


@app.callback(Output('opt_ticker','options'), [Input('opt_sector', 'value')])
def update_ticker_options(selected_sector):
    opts_ticker = get_stock(tbl_name, selected_sector)
    return opts_ticker


@app.callback(Output('output-graph', 'figure'),
             [Input('opt_sector', 'value'),
              Input('opt_ticker', 'value'),
              Input('slider', 'value')])
def update_figure(selected_sector, selected_ticker, selected_dates):
    # selected_sector could be list or str, get the right input for sector selection
    # str = get_sector_choices(selected_sector)
    # handling edge case: when no sector chosen

    if len(selected_sector) == 0:
        query = "SELECT ticker, purchase_date, %(col)s FROM %(tbl_name)s GROUP BY ticker, purchase_date, %(col)s HAVING ticker = %(ticker)s ORDER BY purchase_date;"
        params = {'tbl_name': AsIs(tbl_name_ticker), 'sector': None, 'ticker': selected_ticker, 'col': AsIs(selected_col)}
    elif type(selected_sector) is str:
        query = "SELECT ticker, purchase_date, %(col)s FROM %(tbl_name)s GROUP BY ticker, purchase_date, %(col)s HAVING ticker = %(ticker)s ORDER BY purchase_date;"
        params = {'tbl_name': AsIs(tbl_name_ticker), 'sector': selected_sector, 'ticker': selected_ticker, 'col': AsIs(selected_col)}
    else:
        if len(selected_sector) == 1:
            query = "SELECT ticker, purchase_date, %(col)s FROM %(tbl_name)s WHERE sector = %(sector)s AND ticker = %(ticker)s ORDER BY purchase_date;"
            params = {'tbl_name': AsIs(tbl_name_ticker), 'sector': selected_sector[0], 'ticker': selected_ticker,
              'col': AsIs(selected_col)}
        else:
            query = "SELECT ticker, purchase_date, %(col)s FROM %(tbl_name)s WHERE sector IN %(sector)s AND ticker = %(ticker)s ORDER BY purchase_date;"
            params = {'tbl_name': AsIs(tbl_name_ticker), 'sector': tuple(selected_sector), 'ticker': selected_ticker,
              'col': AsIs(selected_col)}

    df_update = load_data(query, params)

    # get the dates
    start = first_dates.iloc[selected_dates[0]][0]
    end = first_dates.iloc[min(selected_dates[1]-1, N-1)][0]

    # filtering the data
    source = df_update[(df_update.purchase_date > start) & (df_update.purchase_date < end)]
    # updating the plot
    trace_1 = go.Scatter(x=source.purchase_date, y=source[selected_col],
                        name=selected_ticker,
                        line=dict(width=1, color='rgb(229, 151, 50)',),
                         mode='markers')
    fig = go.Figure(data=[trace_1], layout=layout)
    fig.update_xaxes(title_text='Year')
    fig.update_yaxes(title_text='Profit&Loss ($)')

    return fig


server = app.server

if __name__ == '__main__':
    app.run_server(debug=True)