from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, TIMESTAMP
from sqlalchemy.sql import select

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
    with engine.connect() as con:
        table = MetaData(bind=con, reflect=True).tables.get(table_name)
        if table is None:
            return "'{}' table not found".format(table_name)
        try:
            ins = table.insert().values(**data)
            # ins.compile()
            con.execute(ins)
        except Exception as identifier:
            return str(identifier)


def read_table(table_name, check=False):
    with engine.connect() as con:
        table = MetaData(bind=con, reflect=True).tables.get(table_name)
        # Get column name from table
        if table is None:
            msg = "'{}' table not found".format(table_name)
            return msg
        columns = list(map(lambda col: str(col).split('.')[-1], table.columns))
        all_rows = con.execute(select([table])).fetchall()
        mapped_rows = list()

        for row in all_rows:
            mapped_rows.append(dict())
            for pos in range(len(row)):
                if columns[pos] == 'id':
                    continue
                mapped_rows[-1][columns[pos]] = row[pos]

    return mapped_rows
