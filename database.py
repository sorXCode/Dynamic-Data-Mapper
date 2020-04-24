from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, TIMESTAMP
from sqlalchemy.sql import select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy_filters import apply_filters
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_filters.exceptions import FieldNotFound


engine = create_engine('sqlite:///database.db', echo=True)


def create_table(table_name, columns):
    """
    Creates new table with given columns
    """
    metadata = MetaData()

    table = Table(table_name, metadata,
                  Column("id", Integer, primary_key=True), sqlite_autoincrement=True)

    for column_name in columns:
        if column_name.lower() in ['age', 'timestamp']:
            table.append_column(Column(column_name, Integer, nullable=False))
        else:
            table.append_column(Column(column_name, String, nullable=False))

    metadata.create_all(engine)


def update_table(table_name, data):
    """
    :::Updates Table with data:::
    args: 
    table_name: str
    data: {
        "field": "value"
    } 
    """
    with engine.connect() as con:
        table = MetaData(bind=con, reflect=True).tables.get(table_name)
        if table is None:
            return "'{}' table not found".format(table_name)
        try:
            ins = table.insert()
            con.execute(ins, data)
        except Exception as identifier:
            return str(identifier)


def read_table(table_name, filter_spec=False):
    """
    args:   table_name: str,
            filter: list_of_dict or json
    """
    def mapped_rows():
        for row in all_rows:
            pretty_result.append(dict())
            for col in columns:
                if col == 'id':
                    continue
                pretty_result[-1][col] = eval(f"row.{col}")
        return pretty_result

    with engine.connect() as con:
        table = MetaData(bind=con, reflect=True).tables.get(table_name)
        session = sessionmaker(bind=engine)()
        
        # Get column name from table
        if table is None:
            msg = "'{}' table not found".format(table_name)
            return msg
        columns = list(map(lambda col: str(col).split('.')[-1], table.columns))
        
        # construct dynamic table reflection
        Base = declarative_base()
        Base.metadata.reflect(engine)
        table_obj = type("table", (Base,), {"__table__": table})
        session_query = session.query(table_obj)

        if filter_spec:
            try:
                session_query = apply_filters(s, filter_spec)
            except FieldNotFound:
                return "INVALID FILTER"
        
        all_rows = session_query.all()
        pretty_result = list()
        
    return mapped_rows()