#!/usr/bin/env python
# coding: utf-8



import os
import subprocess
from datetime import datetime



'''
this script is used 
to insert records into the table (in my example, a reference table is meant, but this is not necessary) in Сlickhouse without using the 
ClickHouse Python Driver and other drivers to work with Clickhouse using python


'''
    




def click_insert_with_subprocess(data_file, hostname, username, password, table='your_dict_table_name'): 
    """
    This function is necessary to perform an insertion into a table whose name is passed by the last argument
    Parameters
    ----------
    data_file: string
        the filename with data to insert to Clickhouse dict table
    hostname: string 
        hostname

    username: string
        username

    password: string
        user password

    table: string
        the name of the dict table in Clickhouse to which you want to insert data from csv
 
    Returns
    -------
    None 
        
    """
    import shlex, subprocess, pprint

    cmd='''clickhouse-client --host {0} --user {1} --password {2} --query=" INSERT INTO {3} FORMAT CSV"< '{4}' '''.format(hostname, username, password, table, data_file)
    result = subprocess.run(cmd, shell=True)





def log(message):
    '''
    This function is used to log the stages of the ETL process

     Parameters
    ----------
        message: строка 
            message to write to the log file
    
    Returns
    -------
        None 
    '''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("commslogfile1707.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')





def main ():
    
    timestamp_format = '%Y_%m_%d' # Year-Month-day
    now = datetime.now() # get current timestamp
    curr_date = now.strftime(timestamp_format) 

    
    # insert into table 
    data_file=f'data_to_insert_on_{curr_date}.csv'
    hostname='hostname'
    username='username'
    password='your_secret_password'
    log("insert Started")
    click_insert_with_subprocess(data_file, hostname, username, password, table='your_dict_table_name')
    log("insert ended")


if __name__ == '__main__':
    main()

