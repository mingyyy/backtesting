import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go
from secrete import db_password
import psycopg2


# external JavaScript files
external_scripts = [
    'https://www.google-analytics.com/analytics.js',
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

host = 'ec2-3-229-236-236.compute-1.amazonaws.com'
tbl_name = "test"
selected_col = 'purchase_price'


def load_data(query):
    conn= psycopg2.connect(host=host, user='postgres', password=db_password)
    sql_command = (query)
    df = pd.read_sql(sql_command, conn)
    return df


def get_sector(tbl_name):
    q = "SELECT sector FROM {} GROUP BY sector;".format(tbl_name)
    sector_list = load_data(q)['sector']
    opts_sector = [{'label': i, 'value': i} for i in sector_list]
    return opts_sector


def get_stock(tbl_name, sector):
    if len(sector) == 0:
        q = "SELECT ticker FROM {} GROUP BY ticker;".format(tbl_name)
    else:
        q = "SELECT ticker FROM {} GROUP BY sector, ticker HAVING sector IN ({});".format(tbl_name, sector)
    ticker_list = load_data(q)['ticker']
    opts = [{'label':  i, 'value': i} for i in ticker_list]
    return opts


# Define dropdown list.
opts_sector = get_sector(tbl_name)
first_sector = opts_sector[0]['value']
opts_ticker = get_stock(tbl_name, "'" + first_sector + "'")
first_ticker = opts_ticker[0]['value']


# Load Data from Postgres for x, y
q = "SELECT purchase_date, {} FROM {} WHERE ticker='{}' ORDER BY purchase_date;".format(selected_col, tbl_name, first_ticker)
df_init = load_data(q)


# Define Range Slider options
q = "SELECT purchase_date FROM {} GROUP BY purchase_date ORDER BY purchase_date;".format(tbl_name)
first_dates = load_data(q)
start_date = first_dates['purchase_date'].min()
end_date = first_dates['purchase_date'].max()

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
layout = go.Layout(title='Purchase Price Plot', hovermode='closest')
fig = go.Figure(data=[trace_1], layout=layout)


# Create a Dash layout
app.layout = html.Div([
        # Header
        html.Div([
            html.H1("Trading Strategy Back Testing Dashboard"),
            html.H5("MVP testing version 1.1"),],
            style={'paddingLeft': '10%',
                   'paddingBottom':'10%',
                   'paddingTop':'10%',
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
                      'fontSize': '15px',
                      'paddingTop': '10%',
                      'paddingLeft': '10%',
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
                          'fontSize': '15px',
                          'paddingTop': '10%',
                          'paddingLeft': '10%',
                          'display': 'inline-block'}),
        ],
            style={
            'borderBottom': 'thin lightgrey solid',
            'backgroundColor': 'rgb(250, 250, 250)',
            'padding': '5% 5%'
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
                          'fontSize': '30%',
                          'paddingLeft': '10%',
                          'display': 'inline-block',
                          })
    ])


def get_sector_choices(selected_sector):
    # selected_sector could be list or str, get the right input for sector selection
    str = ''
    if type(selected_sector) is list:
        for i in selected_sector:
            str +=  " '" + i + "', "
        str = str[:-2]
    else:
        str = " '" + selected_sector + "' "
    return str


@app.callback(Output('opt_ticker','options'), [Input('opt_sector', 'value')])
def update_ticker_options(selected_sector):
    opts_ticker = get_stock(tbl_name, get_sector_choices(selected_sector))
    return opts_ticker


@app.callback(Output('output-graph', 'figure'),
             [Input('opt_sector', 'value'),
              Input('opt_ticker', 'value'),
              Input('slider', 'value')])
def update_figure(selected_sector, selected_ticker, selected_dates):
    # selected_sector could be list or str, get the right input for sector selection
    str = get_sector_choices(selected_sector)

    # handling edge case: when no sector chosen
    if len(selected_sector) == 0:
        query = "SELECT ticker, purchase_date, {} FROM {} GROUP BY ticker, purchase_date, {} HAVING ticker = '{}' ORDER BY purchase_date;"\
            .format(selected_col, tbl_name, selected_col, selected_ticker)
    else:
        query = "SELECT ticker, purchase_date, {} FROM {} WHERE sector in ({}) AND ticker = '{}' ORDER BY purchase_date;"\
            .format(selected_col, tbl_name, str, selected_ticker)
    df_update = load_data(query)

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
    return fig


server = app.server

if __name__ == '__main__':
    app.run_server(debug=True)