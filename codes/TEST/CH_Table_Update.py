import sys

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import MySql_To_Clickhouse as MySql_CH


table_name = 'EPFO_ESTABLISHMENTS_PAYMENT_MONTHLY_NO_PIT_CLEAN_DATA'
print(table_name)

MySql_CH.ch_truncate_and_create(table_name)