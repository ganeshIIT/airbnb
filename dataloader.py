import numpy as np
import pyodbc

import helper

def timetoseconds(df):
    cols = df.select_dtypes('timedelta64[ns]')
    return (df.assign(**{col:df[col].dt.seconds 
               for col in cols}))

def full_load(df, tbl, hasindex = True, custom={"id":"INT PRIMARY KEY"}):
    if hasindex:
        df = df.reset_index()
    df.index = id= np.arange(1,len(df)+1)
    df = (df.reset_index()
            .rename(columns = {'index': 'id'})
            #.assign(id = lambda x: x['id']+1)
            .pipe(timetoseconds)
            )
    list_df = np.array_split(df, len(df)//1000 +1)
    with pyodbc.connect(helper.get_connstring()) as conn:
        helper.to_sqlserver(df =list_df[0], name=tbl, conn = conn, if_exists="replace", custom=custom, temp=False)
        conn.commit()
        for i in range(1,len(list_df)):
            helper.to_sqlserver(df =list_df[i], name=tbl, conn = conn, if_exists="append", temp=False)
            conn.commit()
    conn.close()
    print(f'DataFrame full loaded to Table: {tbl}')
    