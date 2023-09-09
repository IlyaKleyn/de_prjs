
#!/usr/bin/env python
# coding: utf-8
#import pandas as pd
#import numpy as np
import sys
#import clickhouse_connect
#from clickhouse_driver import Client
#import pandahouse as ph




def click_select_with_subprocess2(asn,cities_list,hostname, username, password): 
    """
    Эта функция нужна, чтобы сделать выборку полей dt (дата), ip_prefix, city для автономной системы, 
    номер которой передается в качестве аргумента в параметр функции asn в формате 'as9049' для городов, список строк 
    которых передается в кач-ве аргумента в параметр функции cities_list
    Parameters
    ----------
    asn: string
        номер автономной системы в формате 'as9049'

    cities_list: list
        список строк городов-источников (определяются по peer_ip_src)

    hostname: string 
        имя хоста

    username: string
        имя пользователя

    password: string
        пароль пользователя
 
    Returns
    -------
    None - в результате работы функции формируются csv - файлы со столбцами date, ip_prefix, city для автономной системы, переданной 
    в параметр asn для города, для каждого из городов в списке cities_list. Файл имеет вид {asn}_bgp_geocommunities_on_{date}_for_{city}.csv
        
    """
    import shlex, subprocess, pprint

    from datetime import datetime
    timestamp_format = '%Y_%m_%d' # Year-Month-day
    now = datetime.now() # get current timestamp
    date = now.strftime(timestamp_format) 
    
    for city in cities_list:
        comm_city_data_file_i=f'{asn}_bgp_geocommunities_on_{date}_for_{city}_without_nulls_in_city.csv'
        cmd='''clickhouse-client --host {hostname} --user {username} --password {password} --query="SELECT comm_mv.dt, comm_mv.ip_prefix, comm_mv.city FROM common_mv_0809_wo_as3216_peers_with_dict3 as comm_mv where comm_mv.asn = '{asn:s}' and comm_mv.city_for_filter = '{city:s}' and not empty(comm_mv.city) INTO OUTFILE '{outfile}' FORMAT CSV;" '''.format(hostname=hostname, username=username, password=password, asn=asn, city=city, outfile=comm_city_data_file_i)
        # cmd=['clickhouse-client', '--host', 'hostname', '--user', username, 
        #     '--password', password, '--query', 
        #     ]
        # .format(hostname, username, password, table, comm_city_data_file)
        result = subprocess.run(cmd, shell=True)
        # print('result of run', result)
        # return result.stdout.decode('latin-1')


def main():
    asn_list=['asXXXX', 'asXYZY', 'asQSVC', 'asFSAZ', 'asJHFT']
    cities_list=['Moscow', 'St.Petersburg']

    # host='localhost'
    # port=8123
    hostname='xx.x.xxx.x'
    username='username'
    password='secretpassword'
    
    for asn in asn_list:
        #conn=connect_to_click2(username, password, hostname)
        #get_asn_bgp_communities_data3(conn, asn, cities_list)
        click_select_with_subprocess2(asn,cities_list, hostname, username, password)
    # ph_select_from_click()
    
        






if __name__ == '__main__':
    main()
