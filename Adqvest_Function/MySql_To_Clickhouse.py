##### Hetesh #####

import os
import subprocess
import paramiko
import pandas as pd


############# ----------------------------------------- ClickHouse DB Connection ------------------------------------------ #############

properties = pd.read_csv(r'C:\Users\Administrator\AdQvestDir\Adqvest_Function\Adqvest_ClickHouse_properties.txt',delim_whitespace=True)

host            = list(properties.loc[properties['Env'] == 'Host'].iloc[:,1])[0]
port            = list(properties.loc[properties['Env'] == 'Port'].iloc[:,1])[0]
db_name         = list(properties.loc[properties['Env'] == 'DB_Name'].iloc[:,1])[0]
user_name       = list(properties.loc[properties['Env'] == 'User_Name'].iloc[:,1])[0]
password_string = list(properties.loc[properties['Env'] == 'Password_String'].iloc[:,1])[0]

############# ----------------------------------------- MySql DB Connection ---------------------------------------------- #############

properties_sql = pd.read_csv(r'C:\Users\Administrator\AdQvestDir\Adqvest_Function\MySql_properties.txt',delim_whitespace=True)

host_sql            = list(properties_sql.loc[properties_sql['Env'] == 'Host'].iloc[:,1])[0]
port_sql            = list(properties_sql.loc[properties_sql['Env'] == 'Port'].iloc[:,1])[0]
db_name_sql         = list(properties_sql.loc[properties_sql['Env'] == 'DB_Name'].iloc[:,1])[0]
user_name_sql       = list(properties_sql.loc[properties_sql['Env'] == 'User_Name'].iloc[:,1])[0]
password_string_sql = list(properties_sql.loc[properties_sql['Env'] == 'Password_String'].iloc[:,1])[0]

############# ----------------------------------------- EC_2 Connection ---------------------------------------------- #############

properties_ec2 = pd.read_csv(r'C:\Users\Administrator\AdQvestDir\Adqvest_Function\Clickhouse_EC2_Machine_Properties.txt',delim_whitespace=True)

host_ec2            = list(properties_ec2.loc[properties_ec2['Env'] == 'Host'].iloc[:,1])[0]
port_ec2            = list(properties_ec2.loc[properties_ec2['Env'] == 'Port'].iloc[:,1])[0]
db_name_ec2         = list(properties_ec2.loc[properties_ec2['Env'] == 'DB_Name'].iloc[:,1])[0]
user_name_ec2       = list(properties_ec2.loc[properties_ec2['Env'] == 'User_Name'].iloc[:,1])[0]
password_string_ec2 = list(properties_ec2.loc[properties_ec2['Env'] == 'Password_String'].iloc[:,1])[0]
key_file_ec2        = list(properties_ec2.loc[properties_ec2['Env'] == 'Key_File_Path'].iloc[:,1])[0]

def ssh_connect():
    ############# ----------------------------------------- Establishing SSH Client -------------------------------------------- #############

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    

    ############# ----------------------------------------- Establishing Connection ----------------------------------------- #############

    ssh.connect(str(host_ec2), username=str(user_name_ec2), password=str(password_string_ec2), key_filename=str(key_file_ec2))
    print("CONNECTION ESTABLISHED")

    return ssh


def ssh_close(ssh):
    ############# ----------------------------------------- Closing Connection ---------------------------------------------- #############
    ssh.close()
    print("CONNECTION CLOSED")


############# ----------------------------------------- Primary Functions ----------------------------------------------- #############

#This Code IS FOR CHECKING IF TABLE EXISTS
def ch_table_check(table_name,ssh):

    query = "clickhouse-client --host localhost --port "+str(port)+" --user "+str(user_name)+" --password '"+str(password_string)+"' --query 'SELECT count(*) FROM AdqvestDB."+str(table_name)+"'"
    stdin, stdout, stderr = ssh.exec_command(query,get_pty = True)
    return stdout

# This Code IS FOR TRUNCATING THE RECORDS
def ch_truncate(table_name,ssh):

    query = "clickhouse-client --host localhost --port "+str(port)+" --user "+str(user_name)+" --password '"+str(password_string)+"' --query 'TRUNCATE TABLE AdqvestDB."+str(table_name)+"'"
    stdin, stdout, stderr = ssh.exec_command(query,get_pty = True)
    return stderr

# This Code IS FOR INSERTING THE RECORDS
def ch_insert(table_name,ssh):

    query = "clickhouse-mysql --src-host="+str(host_sql)+" --src-user="+str(user_name_sql)+" --src-password='"+str(password_string_sql)+"' --migrate-table --src-tables=AdqvestDB."+str(table_name)+" --dst-host=localhost --dst-user="+str(user_name)+" --dst-password='"+str(password_string)+"' --dst-schema "+str(db_name_sql)+" --log-file=AdqvestDB.log"
    stdin, stdout, stderr = ssh.exec_command(query,get_pty = True)
    return stderr

############# ----------------------------------------- NOT TO BE USED WITHOUT INFORMING ----------------------------------------- #############

# This Code IS FOR DELETING ENTIRE TABLE AND SCHEMA
# def ch_drop(table_name,ssh):
#
#     query = "clickhouse-client --host localhost --port "+str(port)+" --user "+str(user_name)+" --password '"+str(password_string)+"' --query 'DROP TABLE AdqvestDB."+str(table_name)+"'"
#     stdin, stdout, stderr = ssh.exec_command(query,get_pty = True)
#     return stderr

############# -------------------------------------------------------------------------------------------------------------------- #############

# This Code IS FOR CREATING NEW TABLE WITH DATA INSERTION
def ch_create_and_insert(table_name,ssh):

    query = "clickhouse-mysql --src-host="+str(host_sql)+" --src-user="+str(user_name_sql)+" --src-password='"+str(password_string_sql)+"' --migrate-table --src-tables=AdqvestDB."+str(table_name)+" --dst-host=localhost --dst-user="+str(user_name)+" --dst-password='"+str(password_string)+"' --dst-create-table --dst-schema "+str(db_name_sql)+" --log-file=AdqvestDB.log"
    stdin, stdout, stderr = ssh.exec_command(query,get_pty = True)

    return stderr


############# ----------------------------------------- FINAL FUNCTIONS --------------------------------------------------------- #############

# This Code IS FOR TRUNCATING THE RECORDS AND INSERTING THE RECORDS
def ch_truncate_and_insert(table_name):

    ssh = ssh_connect()

    stderr_truncate = ch_truncate(table_name,ssh)
    print(stderr_truncate.readlines())
    stderr_insert = ch_insert(table_name,ssh)
    print(stderr_insert.readlines())

    stderr = ch_table_check(table_name,ssh)
    output_check = str(stderr.readlines())
    
    ssh_close(ssh)
    
    if ("DB::Exception:" in output_check):
        raise Exception("SOMETHING WENT WRONG TABLE DOES NOT EXIST")
    else :
        print("TABLE UPDATED IN CLICKHOUSE")

    

# This Code IS FOR CREATING THE TABLE AND INSERTING DATA
def ch_truncate_and_create(table_name):

    ssh = ssh_connect()

    stderr_truncate = ch_truncate(table_name,ssh)
    print(stderr_truncate.readlines())
    stderr = ch_create_and_insert(table_name,ssh)
    print(stderr.readlines())

    stderr = ch_table_check(table_name,ssh)
    output_check = str(stderr.readlines())
    
    ssh_close(ssh)
    
    if ("DB::Exception:" in output_check):
        raise Exception("SOMETHING WENT WRONG PLEASE MANUALLY CHECK")
    else :
        print("TABLE CREATED AND UPDATED IN CLICKHOUSE")

    

############# ----------------------------------------- NOT TO BE USED WITHOUT INFORMING ----------------------------------------- #############

# This Code IS FOR DELETING ENTIRE TABLE AND SCHEMA AND TO CREATE NEW TABLE WITH DATA INSERTION
# def  ch_drop_and_create(table_name):
#   
#     ssh = ssh_connect()

#     stderr_drop = ch_drop(table_name,ssh)
#     print(stderr_drop.readlines())
#     stderr_create = ch_create_and_insert(table_name,ssh)
#     print(stderr_create.readlines())
#
#     stderr = ch_table_check(table_name,ssh)
#     output_check = str(stderr.readlines())
#
#     if ("DB::Exception:" in output_check):
#         raise Exception("SOMETHING WENT WRONG PLEASE MANUALLY CHECK")
#     else :
#        print("TABLE DROPPED, CREATED AND UPDATED IN CLICKHOUSE")
#
#     ssh_close(ssh)
#
############# -------------------------------------------------------------------------------------------------------------------- #############
