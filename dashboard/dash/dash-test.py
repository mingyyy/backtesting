import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
from datetime import datetime as dt
import plotly.graph_objs as go
from secrete import db_password
import psycopg2


import boto3, botocore
from secrete import S3_KEY, S3_SECRET, S3_BUCKET

s3 = boto3.client(
   "s3",
   aws_access_key_id=S3_KEY,
   aws_secret_access_key=S3_SECRET
)

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
    'https://codepen.io/chriddyp/pen/bWLwgP.css',
    {
        'href': 'https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css',
        'rel': 'stylesheet',
        'integrity': 'sha384-MCw98/SFnGE8fJT3GXwEOngsV7Zt27NXFoaoApmYm81iuXoPkFOJwJ8ERdknLPMO',
        'crossorigin': 'anonymous'
    }
]

app = dash.Dash(external_scripts=external_scripts,
                external_stylesheets=external_stylesheets)
db_string = "postgresql+psycopg2://postgres:{}@ec2-3-229-236-236.compute-1.amazonaws.com:5432/test".format(db_password)
db_name = "results"


def load_data(query):
    conn= psycopg2.connect(host='ec2-3-229-236-236.compute-1.amazonaws.com', user='postgres', password=db_password)
    sql_command = (query)

    df_price = pd.read_sql(sql_command, conn)
    return df_price

# Step 1. Load Data from Postgres
query = "SELECT * FROM {} ORDER BY purchase_date;".format(db_name)
df_price = load_data(query)

# Step 2. Define Range Slider options
query = "SELECT purchase_date FROM {} ORDER BY purchase_date;".format(db_name)
dates = load_data(query)
start_date = dates['purchase_date'].min()
end_date = dates['purchase_date'].max()

N = int(dates.count()[0])
# print(N)
date_list = dates['purchase_date'].tolist()

date_mark = {i: dates.iloc[i][0].strftime('%Y-%m') for i in range(0,N,20)}
# print(date_mark)

trace_1 = go.Scatter(x = df_price.purchase_date, y = df_price['purchase_price'],
                    name = 'Purchase Price',
                    line = dict(width = 2,
                                color = 'rgb(229, 151, 50)'),
                     mode = 'markers')
layout = go.Layout(title = 'Time Series Plot',
                   hovermode = 'closest')
fig = go.Figure(data = [trace_1], layout = layout)


# Step 4. Create a Dash layout
app.layout = html.Div(
    children=[
        # Header and some styles
        html.Div([
            html.H1("Trading Strategy Back Testing Dashboard"),
            html.P("MVP testing version 0.1"),],
            style={'padding': '20px',
                   'backgroundColor': '#74F3FF'}),
        #Input field
        html.Div(children='''
        Symbol to graph:'''),
        dcc.Input(id='input', value='', type='text'),

        # Add a plot
        dcc.Graph(id='output-graph', figure=fig),

        # Range slider
        html.P([
            html.Label("Time Period"),
            dcc.RangeSlider(id='slider',
                            marks=date_mark,
                            min=0,
                            max=N,
                            value=[1, N-1])
        ], style={'width': '80%',
                  'fontSize': '20px',
                  'padding-left': '100px',
                  'display': 'inline-block'})
    ]
)

# Step 4. Add callback functions to render the page
@app.callback(Output('output-graph', 'figure'),
             [Input('input', 'value'),Input('slider', 'value')])
def update_figure(input1, input2):

    query = "SELECT purchase_date FROM {} ORDER BY purchase_date;".format(db_name)
    dates = load_data(query)

    # filtering the data

    st2 = df_price[(df_price.purchase_date > dates.iloc[input2[0]][0]) & (df_price.purchase_date < dates.iloc[input2[1]][0])]

    # updating the plot
    trace_1 = go.Scatter(x = st2.purchase_date, y = st2['purchase_price'],
                        name = 'Stock purchase',
                        line = dict(width = 2,
                                    color = 'rgb(229, 151, 50)',),
                         mode = 'markers')
    selected_ticker = input1.upper()
    trace_2 = go.Scatter(x = st2.purchase_date, y = st2['purchase_price'],#[df_price['ticker'] == selected_ticker],
                        name = input1,
                        line = dict(width = 2,
                                    color = 'rgb(106, 181, 135)',),
                        mode='markers'
                        )
    fig = go.Figure(data=[trace_1, trace_2], layout=layout)
    return fig


# @app.callback(
#     Output(component_id='output-graph',component_property='children'),
#     [Input(component_id='input',component_property='value')]
#     )
# def update_graph(input_data):
#     query = "SELECT * FROM {};".format(db_name)
#     df_price = load_data(query)
#     selected_ticker = input_data.upper()
#
#     selected_col1 = 'purchase_price'
#     selected_col2 = 'pnl'
#
#     # list_purchase = df_price[selected_col1][df_price['ticker'] == selected_ticker]
#     # list_pnl = df_price[selected_col2][df_price['ticker'] == selected_ticker]
#     # list_date = df_price['purchase_date'][df_price['ticker'] == selected_ticker]
#     list_purchase = df_price[selected_col1]
#     list_pnl = df_price[selected_col2]
#     list_date = df_price['purchase_date']
#
#     return dcc.Graph(id='example',
#               figure={
#                   'data':[{'x':list_date,
#                            'y':list_purchase,
#                            'type':'line',
#                            'name': selected_col1},
#                           {'x': list_date,
#                            'y': list_pnl,
#                            'type': 'bar',
#                            'name': selected_col2}
#                           ],
#                   'layout':{
#                       'title': '{} historical data'.format(selected_ticker)
#                   }
#     })



def upload_file_to_s3(file, bucket_name, acl="public-read"):
    """
    Docs: http://boto3.readthedocs.io/en/latest/guide/s3.html
    """
    try:
        s3.upload_fileobj(
            file,
            bucket_name,
            file.filename,
            ExtraArgs={
                "ACL": acl,
                "ContentType": file.content_type
            }
        )
    except Exception as e:
        print("Something is not right: ", e)
        return e

    return "{}{}".format('http://{}.s3.amazonaws.com/'.format(S3_BUCKET), file.filename)



if __name__ == '__main__':
    app.run_server(debug=True)