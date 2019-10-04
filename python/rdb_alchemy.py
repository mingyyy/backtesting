from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, MetaData, Numeric, DateTime
from secrete import db_password, end_point, db_name
from datetime import datetime as dt
db_string = f"postgresql+psycopg2://postgres:{db_password}@{end_point}:5432/"+ db_name

db = create_engine(db_string)

meta = MetaData(db)
res_table = Table('results', meta,
                       Column('strategy_id', String),
                       Column('strategy_name', String),
                       Column('invest_amount', Numeric),
                       Column('PnL', Numeric),
                       Column('TimeStamp', DateTime),

                   )


with db.connect() as conn:

    # Create
    res_table.create()
    insert_statement = res_table.insert().values(strategy_id="s1", strategy_name="buy_on_threshold", invest_amount=100,
                                                 PnL=0, TimeStamp=dt.now())
    conn.execute(insert_statement)

    # Read
    select_statement = res_table.select()
    result_set = conn.execute(select_statement)
    for r in result_set:
        print(r)

    # Update
    update_statement = res_table.update().where(res_table.c.year=="2016").values(title = "Some2016Film")
    conn.execute(update_statement)

    # Delete
    delete_statement = res_table.delete().where(res_table.c.year == "2016")
    conn.execute(delete_statement)