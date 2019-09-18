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

# cursor.execute('''DROP TABLE tutorials;''')
# conn.commit()

# create a new table with a single column called "name"
cursor.execute('''CREATE TABLE tutorials(name char(4));''')
conn.commit()
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
cursor.execute("""SELECT * from tutorials;""")
conn.commit()

rows = cursor.fetchall()
print(rows)

cursor.execute('''DROP TABLE tutorials;''')
conn.commit()

cursor.close()
conn.close()

