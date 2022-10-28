import pandas as pd 
import numpy as np
import pyodbc
from functools import wraps
import re

from sqlalchemy import create_engine

import errors

# Global
DTYPE_MAP = {
    "int64": "int",
    "float64": "float",
    "object": "varchar(max)",
    "datetime64[ns]": "datetime2",
    "bool": "bit",
    "boolean": "bit",
    # To do - map timedelta64[ns] to seconds or string
}

def get_connstring(driver= '{ODBC Driver 17 for SQL Server}'):   
    """
    usage:
    
    cstring = get_connstring()
    with pyodbc.connect() as conn:
        ## code
    """
     
    with open('settings.txt', mode='r') as f:
        cs = f.readline().replace('\n', '')
    d = dict(x.split(':') for x in cs.split(' '))
    #print(d)
    server = d['server']
    username = d['username']
    password = d['password']
    database = d['db']
    cs = 'DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
    return cs


def sqlalchemyengine(driverversion=17):
    """
    This uses sqlalchemy and returns the engine
    """
    with open('settings.txt', mode='r') as f:
        cs = f.readline().replace('\n', '')
    d = dict(x.split(':') for x in cs.split(','))
    print(d)
    server = d['server']
    username = d['username']
    password = d['password'].replace('@', '%40')
    database = d['db']
    engine = create_engine(f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver=ODBC+Driver+{driverversion}+for+SQL+Server")
    return engine


def _cleanlabel(label):
    return re.sub(r'[^\w\s]', '', str(label)).lower().replace(' ', '_')

def cleanlabel(method):
    """
    Doc here
    """
    @wraps(method)
    def method_wrapper(*args, **kwargs):
        df = method(*args, **kwargs)
        df.columns = [
            _cleanlabel(col) for col in df.columns
        ]
        if df.index.name:
            df.index.rename(
                _cleanlabel(df.index.name),
                inplace=True
            )
        return df
    return method_wrapper

def int64_to_uint8(df):
    cols = df.select_dtypes('int64')
    return (df.astype({col:'uint8' for col in cols}))

def datetime64_to_date(df):
    cols = df.select_dtypes('datetime64[ns]').columns
    for col in cols:
        df[col] = df[col].dt.normalize()
    return df

def flatten_cols(df):
    cols = ['_'.join(map(str, vals))
            for vals in df.columns.to_flat_index()]
    df.columns = cols
    return df

def get_row_count(*dfs):
    return [df.shape[0] for df in dfs]

def timetoseconds(df):
    """ Converts timedelta to seconds to push to the db tables"""
    cols = df.select_dtypes('timedelta64[ns]')
    return (df.assign(**{col:df[col].dt.seconds 
               for col in cols}))


def validate_df(columns, instance_method=True):
    """
    Doc here
    """
    def method_wrapper(method):
        @wraps(method)
        def validate_wrapper(self, *args, **kwargs):
            # functions and static methods don't pass self
            # so self is the first positional argument in that case
            df = (self, *args)[0 if not instance_method else 1]
            if not isinstance(df, pd.DataFrame):
                raise ValueError("You should pass a pandas DataFrame")
            if columns.difference(df.columns):
                raise ValueError(
                    f'DataFrame must contain the following columns: {columns}'
                )
            return method(self, *args, **kwargs)
        return validate_wrapper
    return method_wrapper


def window_calc(df, func, agg_dict, *args, **kwargs):
    """
        Perform window calculations
    """
    return df.pipe(func, *args, **kwargs).agg(agg_dict)


def _check_duplicate_cols(df):
    """Returns duplicate column names (case insensitive)
    """
    cols = [c.lower() for c in df.columns]
    dups = [x for x in cols if cols.count(x) > 1]
    if dups:
        raise errors.DuplicateColumns(f"There are duplicate column names. Repeated names are: {dups}. SQL Server dialect requires unique names (case insensitive).")

def _clean_col_name(column):
    """Removes special characters from column names
    """
    column = str(column).replace(" ", "_").replace("(","").replace(")","").replace("[","").replace("]","")
    column = f"[{column}]"
    return column

def _clean_custom(df, custom):
    """Validate and clean custom columns
    """
    for k in list(custom):
        clean_col = _clean_col_name(k)
        if clean_col not in df.columns:
            raise errors.CustomColumnException(f"Custom column {k} is not in the dataframe.")
        custom[clean_col] = custom.pop(k)
    return custom
    
def _get_data_types(df, custom):
    """Get data types for each column as dictionary
    Handles default data type assignment and custom data types
    """
    data_types = {}
    for c in list(df.columns):
        if c in custom:
            data_types[c] = custom[c]
            continue
        dtype = str(df[c].dtype)
        if dtype not in DTYPE_MAP:
            data_types[c] = "varchar(255)"
        else:
            data_types[c] = DTYPE_MAP[dtype]
    return data_types

def _get_default_schema(cur: pyodbc.Cursor) -> str:
    """Get the default schema of the caller
    """
    return str(cur.execute("select SCHEMA_NAME() as scm").fetchall()[0][0])

def _get_schema(cur: pyodbc.Cursor, table_name: str):
    """Get schema and table name - returned as tuple
    """
    t_spl = table_name.split(".")
    if len(t_spl) > 1:
        return t_spl[0], ".".join(t_spl[1:])
    else:
        return _get_default_schema(cur), table_name

def _clean_table_name(table_name):
    """Cleans the table name
    """
    return table_name.replace("'","''")

def _check_exists(cur,schema,table,temp):
    """Check in conn if table exists
    """
    if temp:
        return cur.execute(
            f"IF OBJECT_ID('tempdb..#[{table}]') IS NOT NULL select 1 else select 0"
        ).fetchall()[0][0]
    else:
        return cur.execute(
            f"IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}' and TABLE_SCHEMA = '{schema}') select 1 else select 0"
        ).fetchall()[0][0]

def _generate_create_statement(schema, table, cols, temp):
    """Generates a create statement
    """
    cols = ",".join([f'\n\t{k} {v}' for k, v in cols.items()])
    schema_if_temp = f"[#{table}]" if temp else f"[{schema}].[{table}]"
    return f"create table {schema_if_temp}\n({cols}\n)"

def _check_parameter_if_exists(if_exists):
    """Raises an error if parameter 'if_exists' is not correct
    """
    if if_exists not in ('append', 'fail', 'replace'):
        raise errors.WrongParam(f"Incorrect parameter value {if_exists} for 'if_exists'. Can be 'append', 'fail', or 'replace'")

## function to create or replace a table with pandas DataFrame in SQL server       
def to_sqlserver(df, name, conn, if_exists='append', custom=None, temp=False, copy=False):
    """Main fast_to_sql function.
    Writes pandas dataframe to sql server using pyodbc fast_executemany
    
    
    df: pandas DataFrame to upload
    name: String of desired name for the table in SQL server
    conn: A valid pyodbc connection object
    if_exists: Option for what to do if the specified table name already exists in the database. If the table does not exist a new one will be created. By default this option is set to 'append'
    'append': Appends the dataframe to the table if it already exists in SQL server.
    'fail': Purposely raises a FailError if the table already exists in SQL server.
    'replace': Drops the old table with the specified name, and creates a new one. Be careful with this option, it will completely delete a table with the specified name in SQL server.
    custom: A dictionary object with one or more of the column names being uploaded as the key, and a valid SQL column definition as the value. The value must contain a type (INT, FLOAT, VARCHAR(500), etc.), and can optionally also include constraints (NOT NULL, PRIMARY KEY, etc.)
    Examples: {'ColumnName':'varchar(1000)'} {'ColumnName2':'int primary key'}
    temp: Either True if creating a local sql server temporary table for the connection, or False (default) if not.
    copy: Defaults to False. If set to True, a copy of the dataframe will be made so column names of the original dataframe are not altered.
    """
    
    if copy:
        df = df.copy()
    
    # Assign null custom
    if custom is None:
        custom = {}

    # Handle series
    if isinstance(df, pd.Series):
        df = df.to_frame()

    # Clean table name
    name = _clean_table_name(name)

    # Clean columns
    columns = [_clean_col_name(c) for c in list(df.columns)]
    df.columns = columns

    # Check for duplicate column names 
    _check_duplicate_cols(df)
    custom = _clean_custom(df, custom)

    # Assign data types
    data_types = _get_data_types(df, custom)

    # Get schema
    cur = conn.cursor()
    schema, name = _get_schema(cur, name)
    if schema == '':
        schema = cur.execute("SELECT SCHEMA_NAME()").fetchall()[0][0]
    exists = _check_exists(cur, schema, name, temp)

    # Handle existing table
    create_statement = ''
    if exists:
        _check_parameter_if_exists(if_exists)
        if if_exists == "replace":
            cur.execute(f"drop table [{schema}].[{name}]")
            create_statement = _generate_create_statement(schema, name, data_types, temp)
            cur.execute(create_statement)
        elif if_exists == "fail":
            fail_msg = f"Table [{schema}].[{name}] already exists." if temp else f"Temp table #[{name}] already exists in this connection"
            raise errors.FailError(fail_msg)
    else:
        create_statement = _generate_create_statement(schema, name, data_types, temp)
        cur.execute(create_statement)

    # Run insert
    if temp:
        insert_sql = f"insert into [#{name}] values ({','.join(['?' for v in data_types])})"
    else:
        insert_sql = f"insert into [{schema}].[{name}] values ({','.join(['?' for v in data_types])})"
    insert_cols = df.values.tolist()
    insert_cols = [[None if type(cell) == float and np.isnan(cell) else cell for cell in row] for row in insert_cols]
    cur.fast_executemany = True
    cur.executemany(insert_sql, insert_cols)
    cur.close()
    return create_statement


if __name__ == "__main__":
    # using connection function
    cstring = get_connstring()
    with pyodbc.connect(cstring) as conn:
        query = "SELECT top 10 * from listings"
        df = pd.read_sql(query, conn)
        print(df)

    ## using sqlalchemy
    # engine = sqlalchemyengine()
    # connection = engine.connect()
    # query = "SELECT top 10 * from health"
    # df = pd.read_sql(query, connection)
    # print(df)
    # connection.close()
        
        
