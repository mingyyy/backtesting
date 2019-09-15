import psycopg2
from secrete import db_password, db_user_name, end_point, db_id


# use our connection values to establish a connection
conn = psycopg2.connect(
    database='postgres',
    user="postgres",
    password=db_password,
    host=end_point,
    port='5432'
)
# create a psycopg2 cursor that can execute queries
cursor = conn.cursor()
# create a new table with a single column called "name"
# cursor.execute('''CREATE TABLE tutorials(name char(40);)''')
# conn.commit()
# cursor.execute("""CREATE TABLE historical_prices(
#             id serial PRIMARY KEY,
#             date DATE NOT NULL,
#             ticker VARCHAR (5) NOT NULL,
#             open real,
#             close real,
#             adj_close real,
#             low real,
#             high real,
#             volume INT);""")

cursor.execute("""INSERT INTO tutorials (name) VALUES ('APPL');""")
# run a SELECT statement - no data in there, but we can try it
cursor.execute("""SELECT * from tutorials""")
conn.commit()  # <--- makes sure the change is shown in the database
rows = cursor.fetchall()
print(rows)
cursor.close()
conn.close()

