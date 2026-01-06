# -*- coding: utf-8 -*-
"""
Created on Sat Jun 14 13:27:19 2025

@author: GOKUL
"""

import warnings
warnings.filterwarnings('ignore')

from clickhouse_driver import Client
import time

# Connect to old and new ClickHouse servers
old_client = Client(
    host='ec2-34-219-54-24.us-west-2.compute.amazonaws.com',
    user='default',
    password='Clickhouse@2024',
    database='AdqvestDB'
)

new_client = Client(
    host='ec2-52-88-156-240.us-west-2.compute.amazonaws.com',
    user='default',
    password='Clickhouse@2024',
    database='AdqvestDB'
)

# Set batch size
batch_size = 15000
offset = 0

# Get total record count from old server
total_records = old_client.execute("SELECT COUNT(*) FROM NSE_INVESTOR_INFORMATION_PAST_DATA_CORPUS_CHUNKED")[0][0]
print(f"Total records to transfer: {total_records}")

from datetime import date, datetime

def clean_rows(rows):
    cleaned = []
    for row in rows:
        row = list(row)
        # Indices for dates — adjust if needed
        document_date_idx = 9
        relevant_date_idx = 10

        # Replace None with a safe default (you can choose `date.min`, or skip record)
        if row[document_date_idx] is None:
            row[document_date_idx] = date(1970, 1, 1)
        if row[relevant_date_idx] is None:
            row[relevant_date_idx] = datetime(1970, 1, 1)

        cleaned.append(tuple(row))
    return cleaned


# Fetch and insert in batches
while offset < total_records:
    select_query = f"""
        SELECT 
            document_id,
            symbol,
            Sector,
            Industry,
            Industry_Sub_Category,
            Industry_Sub_Category_2,
            document_company,
            document_type,
            document_year,
            document_date,
            relevant_date,
            document_link,
            page_number,
            document_content,
            document_content_modify,
            embedding,
            vector_db_status
        FROM NSE_INVESTOR_INFORMATION_PAST_DATA_CORPUS_CHUNKED
        LIMIT {batch_size} OFFSET {offset}
    """

    # Execute on old server
    rows = old_client.execute(select_query)
    

    # Insert into new server
    if rows:
        safe_rows = clean_rows(rows)  # 👈 Cleaned version
        new_client.execute(
            """
            INSERT INTO NSE_INVESTOR_INFORMATION_PAST_DATA_CORPUS_CHUNKED_backup_gokul (
                document_id,
                symbol,
                Sector,
                Industry,
                Industry_Sub_Category,
                Industry_Sub_Category_2,
                document_company,
                document_type,
                document_year,
                document_date,
                relevant_date,
                document_link,
                page_number,
                document_content,
                document_content_modify,
                embedding,
                vector_db_status
            ) VALUES
            """, safe_rows
        )
        print(f"✅ Inserted rows {offset} to {offset + len(rows)}")
    else:
        print(f"⚠️ No rows returned at offset {offset} — stopping.")
        break

    offset += batch_size
    time.sleep(2)  # optional cooldown
