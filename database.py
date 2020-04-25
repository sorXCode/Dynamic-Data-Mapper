from sqlalchemy import (TIMESTAMP, Column, Integer, MetaData, String, Table,
                        create_engine)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_filters import apply_filters
from sqlalchemy_filters.exceptions import FieldNotFound

ENGINE = create_engine('sqlite:///database.db', echo=True)


def create_table(table_name, columns):
    """
    Creates new table with given columns
    args:
        table_name: string/int
        columns: dict() -> {'age': integer, 'name':string}
    """
    if get_table(table_name) is not None:
        return "TABLE '{}' EXISTS".format(table_name)

    metadata = MetaData()
    table = Table(table_name, metadata,
                  Column("id", Integer, primary_key=True), sqlite_autoincrement=True)
    column_types = {
        'string': String,
        'integer': Integer,
        'timestamp': Integer,
        # 'timestamp': TIMESTAMP
    }
    try:
        for column_name, column_dtype in columns.items():
            table.append_column(
                Column(column_name, column_types[column_dtype.lower()], nullable=False))
        metadata.create_all(ENGINE)
        return "success"
    except Exception:
        return str(Exception)


def update_table(table_name, data):
    """
    :::Updates Table with data:::
    args:
    table_name: str
    data: {
        "field": "value"
    }
    """
    with ENGINE.connect() as con:
        table = get_table(table_name)
        if table is None:
            return "TABLE '{}' NOT FOUND".format(table_name)
        try:
            ins = table.insert()
            con.execute(ins, data)
        except Exception:
            return str(Exception)
    return "success"


def read_table(table_name, filter_spec=None):
    """
    args:   table_name: str,
            filter: list_of_dict or json
    """
    # with ENGINE.connect() as con:
    #     table = MetaData(bind=con, reflect=True).tables.get(table_name)
    table = get_table(table_name)
    if table is None:
        return "TABLE '{}' NOT FOUND".format(table_name)

    # construct dynamic table reflection for ORM
    session = sessionmaker(bind=ENGINE)()
    Base = declarative_base()
    Base.metadata.reflect(ENGINE)
    table_obj = type("table", (Base,), {"__table__": table})
    session_query = session.query(table_obj)

    if filter_spec:
        try:
            session_query = apply_filters(session_query, filter_spec)
        except FieldNotFound:
            return "INVALID FILTER"

    # # Get column name from table
    columns = list(column.key for column in table_obj.__table__.columns)
    all_rows = session_query.all()
    return prettify_result(columns, all_rows)


def get_table(table_name):
    """
    Gets provided table_name schema from data base
    """
    with ENGINE.connect() as con:
        table = MetaData(bind=con, reflect=True).tables.get(table_name)
    return table


def prettify_result(columns, rows):
    pretty_result = list()
    for row in rows:
        pretty_result.append(dict())
        for col in columns:
            # skipping `id` column
            if col == 'id':
                continue
            pretty_result[-1][col] = eval(f"row.{col}")
    return pretty_result
