import sqlalchemy
import sys
from sqlalchemy import text
import pandas as pd
import adqvest_db
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')

engine = adqvest_db.db_conn()
cursor = engine.connect()

def check_duplicates(df,table_name):
    
    upload = 0
    
    df.drop(['Runtime'],axis=1,inplace=True)

    # Get the column names of the table
    q1 = f"SHOW COLUMNS FROM {table_name};"
    table_info = pd.read_sql(sql = text(q1), con = cursor)
    #print(table_info)
    columns = [f'`{column}`' for column in table_info['Field'] if column not in ('Runtime')]
    #print(columns)

    if len(columns) == 0:
        print("Table does not exist or has no columns.")
        cursor.close()
        return

    # Generate the column names for the GROUP BY clause
    group_by_columns = ', '.join(columns)

    # Count the number of duplicate records
    q2 = f"SELECT * FROM {table_name} WHERE Relevant_Date = '{df['Relevant_Date'][0]}';"
    latest_df =pd.read_sql(sql = text(q2), con = cursor)
    latest_df.drop(['Runtime'],axis=1,inplace=True)


    if bool((latest_df.reset_index(drop=True) == df.reset_index(drop=True)).all):
        print("Data already updated.")
        cursor.close()
        return
    else:
        upload = 1
        
    return upload



