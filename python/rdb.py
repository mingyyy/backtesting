import psycopg2
from secrete import db_password, db_user_name, end_point

# use our connection values to establish a connection
conn = psycopg2.connect(
    database='postgres',
    user="postgres",
    password=db_password,
    host="ec2-3-229-236-236.compute-1.amazonaws.com",
    port='5432'
)
# create a psycopg2 cursor that can execute queries
cursor = conn.cursor()

# cursor.execute('''DROP TABLE results;''')
# conn.commit()

#create a new table with a single column called "name"
cursor.execute('''CREATE TABLE IF NOT EXISTS results(
                                        id serial PRIMARY KEY,
                                        strategy_name VARCHAR(50),
                                        ticker VARCHAR(5),
                                        purchase_date DATE,
                                        purchase_price NUMERIC(10, 2),
                                        purchase_vol NUMERIC(10, 2),
                                        PnL NUMERIC(10, 2)
                );''')
conn.commit()


# cursor.execute("""INSERT INTO results (strategy_name, ticker, purchase_date, purchase_price, purchase_vol, PnL)
#                 VALUES ('first_month_ma', 'APPL', '2018-01-01', 100, 1, 200);""")
cursor.execute("""SELECT * from results;""")
conn.commit()

rows = cursor.fetchall()
print(rows)

# cursor.execute('''DROP TABLE xxxxx;''')
# conn.commit()

cursor.close()
conn.close()

