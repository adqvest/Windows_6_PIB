import os
import sys

import pandas as pd
import datetime as datetime
from pytz import timezone

import smtplib
from openpyxl import load_workbook

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

pd.options.display.max_columns = None
pd.options.display.max_rows = None

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log

engine = adqvest_db.db_conn()
connection = engine.connect()

from clickhouse_driver import Client
host = 'ec2-52-88-156-240.us-west-2.compute.amazonaws.com'
client = Client(host, user='default', password='Clickhouse@2024', database='AdqvestDB')

india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

run_time = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))

import datetime
import pandas as pd


# Define multiple recipients
to = ['mrinalini@adqvest.com','karthik@adqvest.com']
cc = ['s.heteshkumar@adqvest.com','gokulakrishnan.j@adqvest.com']

msg = MIMEMultipart()
msg['From'] = "adqvest.insights@adqvest.com"
msg['To'] = ", ".join(to)  # Join multiple recipients with comma
msg['Cc'] = ", ".join(cc)  # Join CC recipients with comma

# Combine all recipients for smtp.sendmail()
recipients = to + cc

msg['Subject'] = "Company Count of Inv. Presentation and Conf. Call documents on " + str(today.date())
body = "Hello , Please find the Excel Sheet Attached with this Email" 
msg.attach(MIMEText(body, 'plain'))

sel_time = (datetime.datetime.now(india_time) - datetime.timedelta(1)).strftime("%Y-%m-%d %H:%M:%S")
sel_end_time = (datetime.datetime.now(india_time) - datetime.timedelta(hours = 2)).strftime("%Y-%m-%d %H:%M:%S")
time_now = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))

# NSE data

Ch = '''WITH
        all_companies AS (
            SELECT DISTINCT lower(document_company) AS company_lower
            FROM NSE_INVESTOR_INFORMATION_PAST_DATA_CORPUS_CHUNKED
            UNION ALL
            SELECT DISTINCT lower(document_company) AS company_lower
            FROM thurro_pdf_documents_vector_db_final_2
        ),
        
        -- Step 2: Cross join companies with the 2 subject types
        company_subjects AS (
            SELECT
                company_lower,
                subject
            FROM all_companies
            CROSS JOIN (
                SELECT 'Investor Presentation' AS subject
                UNION ALL
                SELECT 'Conference Call'
            ) s
        ),
        
        -- Step 3: All docs with subject tag (searching in document_content)
        all_docs AS (
            SELECT
                lower(document_company) AS company_lower,
                document_content,
                document_year,
                document_id,
                CASE
                    WHEN lower(document_content) LIKE '%investor presentation%' THEN 'Investor Presentation'
                    WHEN lower(document_content) LIKE '%con. call%' OR lower(document_content) LIKE '%con. calls%' OR lower(document_content) LIKE '%conference call%' THEN 'Conference Call'
                    ELSE NULL
                END AS subject
            FROM NSE_INVESTOR_INFORMATION_PAST_DATA_CORPUS_CHUNKED
            UNION ALL
            SELECT
                lower(document_company) AS company_lower,
                document_content,
                document_year,
                document_id,
                CASE
                    WHEN lower(document_content) LIKE '%investor presentation%' THEN 'Investor Presentation'
                    WHEN lower(document_content) LIKE '%con. call%' OR lower(document_content) LIKE '%con. calls%' OR lower(document_content) LIKE '%conference call%' THEN 'Conference Call'
                    ELSE NULL
                END AS subject
            FROM thurro_pdf_documents_vector_db_final_2
        ),
        
        -- Step 4: Final join to link all companies + subjects with docs (LEFT JOIN keeps all companies/subjects)
        final_docs AS (
            SELECT
                cs.company_lower AS document_company,
                cs.subject,
                d.document_year,
                d.document_id
            FROM company_subjects cs
            LEFT JOIN all_docs d
                ON cs.company_lower = d.company_lower AND cs.subject = d.subject
        )
        
        -- Step 5: Pivot counts for Q1 FY25 to Q4 FY25
        SELECT
            document_company AS company,
            subject,
            NULLIF(countDistinctIf(document_id, document_year = 'Q4 FY25'), 0) AS `Q4 FY25`
        FROM final_docs
        GROUP BY document_company, subject
        ORDER BY document_company, subject;''' 
a,cols = client.execute(Ch,with_column_types=True)
CH_df = pd.DataFrame(a, columns=[tuple[0] for tuple in cols])
CH_df['company'] = CH_df['company'].str.title()
CH_df.rename(columns={'company': 'Company','subject':'Subject'}, inplace=True)

excel_path = 'NSE_COMPANYWISE_INV_PRESENTATION_CONF_CALL_Q4_FY25.xlsx'
CH_df.to_excel(excel_path, index=False)

wb = load_workbook(excel_path)
ws = wb.active

for column_cells in ws.columns:
    max_length = 0
    column = column_cells[0].column_letter  # Get the column name (e.g. 'A', 'B')
    for cell in column_cells:
        try:
            if cell.value:
                max_length = max(max_length, len(str(cell.value)))
        except:
            pass
    adjusted_width = max_length + 2  # add a little padding
    ws.column_dimensions[column].width = adjusted_width

wb.save(excel_path)
    
# Attach Count file
part = MIMEBase('application', "octet-stream")
part.set_payload(open("NSE_COMPANYWISE_INV_PRESENTATION_CONF_CALL_Q4_FY25.xlsx", "rb").read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', 'attachment; filename="NSE_COMPANYWISE_INV_PRESENTATION_CONF_CALL_Q4_FY25.xlsx"')
msg.attach(part)

# Send email
smtp = smtplib.SMTP('smtp.gmail.com:587')
smtp.starttls()
smtp.login("thurro@adqvest.com", "csmg uviz lshd yokj")
smtp.sendmail(msg['From'], recipients, msg.as_string())
smtp.quit()
print('REPORT SENT')