import psycopg2
from secrete import db_password, db_user_name, end_point


def connect_DB(tbl, dict):
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

    create_tbl(cursor, conn, tbl, dict)
    # cursor.execute("SELECT * from {};".format(tbl))
    # conn.commit()
    # rows = cursor.fetchall()
    # print(rows)
    cursor.close()
    conn.close()


def drop_tbl(cursor, conn, tbl):
    cursor.execute('DROP TABLE {};'.format(tbl))
    conn.commit()
    return 'Table {} has been dropped.'.format(tbl)


def create_tbl(cursor, conn, tbl, dict):
    str = ''
    for k, v in dict.items():
        str += k + ' ' + v + ', '
    str = str[:-2]

    #create a new table to store the field type suggestions
    cursor.execute('CREATE TABLE {} (ID SERIAL PRIMARY KEY, {});'.format(tbl, str))
    conn.commit()

    return 'Table {} has been created.'.format(tbl)


if __name__ == '__main__':
    # testing table creation
    connect_DB('TBL_SCHEMA', {'FILE_NAME': 'VARCHAR(100)','COL_NAME':'VARCHAR(100)','COL_TYPE_SUGGEST': 'VARCHAR(20)','COL_TYPE_FINAL': 'VARCHAR(20)','USER_ID': 'VARCHAR(10)','CREATE_DATE': 'TIMESTAMP'})
