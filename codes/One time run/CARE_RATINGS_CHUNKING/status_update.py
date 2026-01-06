import sys
import pandas as pd
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Windows_3/Adqvest_Function')
import adqvest_db
engine = adqvest_db.db_conn()
connection = engine.connect()

from clickhouse_driver import Client
host = 'ec2-52-11-204-251.us-west-2.compute.amazonaws.com'
user_name = 'default'
password_string = 'Clickhouse@2024'
db_name = 'AdqvestDB'

client = Client(host, user=user_name, password=password_string, database=db_name)


query = "select distinct document_link from thurro_pdf_documents_vector_db_care_ratings"
rd,cols = client.execute(query,with_column_types=True)
df = pd.DataFrame(rd, columns=[tuple[0] for tuple in cols])


for i in range(1,len(df)):
    print(i)
    link = df['document_link'][i]
    update_query = f"update CARE_RATINGS_DAILY_FILES_LINKS_CHUNKING set Chunked = 'Yes' where Links = '{link}'"
    update_query = update_query.replace('%','%%')
    print(update_query)
    connection.execute(update_query)
    connection.execute('commit')