import warnings
warnings.filterwarnings('ignore')

from clickhouse_driver import Client
host = 'ec2-52-88-156-240.us-west-2.compute.amazonaws.com'
client = Client(host, user='default', password='Clickhouse@2024', database='AdqvestDB')

batch_size = 15000
offset = 0

total_records = client.execute("""
    SELECT COUNT(*)
    FROM BSE_ANNOUNCEMENT_ANNUAL_REPORT_COMPANY_WISE_YEARLY_DATA_CORPUS_CHUNKED
""")
total_records = total_records[0][0]
print(total_records)

while offset < total_records:
    query = f"""
        INSERT INTO AdqvestDB.BSE_ANNOUNCEMENT_ANNUAL_REPORT_COMPANY_WISE_YEARLY_DATA_CORPUS_CHUNKED_NEW
        SELECT
            Document_Id, Chunk_Id, Symbol, Sector, Industry, Industry_Sub_Category,
            Industry_Sub_Category_2, Document_Company, Document_Type, Document_Year,
            Document_Date, Published_Date, Document_Link, Page_Number, Document_Content,
            Embedding, Vector_Db_Status, Runtime_Scraped, Runtime_Chunking,
            Runtime_Milvus, Runtime_Final_Corpus
        FROM AdqvestDB.BSE_ANNOUNCEMENT_ANNUAL_REPORT_COMPANY_WISE_YEARLY_DATA_CORPUS_CHUNKED
        LIMIT {batch_size} OFFSET {offset}
    """
    client.execute(query)
    print(f"Inserted batch from {offset} to {offset + batch_size}")
    offset += batch_size
    import time
    time.sleep(20)

 