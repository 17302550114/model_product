# 使用
import os
import re
import sys
import random
import datetime
import pandas as pd
import psycopg2
from dateutil import parser
from faker import Faker
from pathlib import Path
sys.path.append(os.getcwd())

print(os.getcwd())
from sql_conn import *
from multiprocessing import *
from tools.other_tool import *
def error(error):
    print(error)

conn_mysql = db_conn_init("db_mysql_local")
now = datetime.datetime.now()
model_path = '人员相关/'
file_path =  PROJECT_BASE_PATH +  '/model_product/logs/' + model_path
my_logger(file_path,"更新关注人员专题库.log")


def GetAJXXFromAJBH(list_ajbh):
    data_ajbh = pd.DataFrame(list_ajbh,columns=["AJBH"])
    tmp_table_name = "tmp_table_ajbh" + str(np.random.random())[2:10]
    sql = f'''
        select aj.ajbh,aj.ajlb,aj.slsj,aj.ajmc,fasjsx,fasjxx,jd aj_jd,wd aj_wd
        from aj_jbxx aj
        inner join {tmp_table_name} ajbh on ajbh.ajbh = aj.ajbh
    '''
    tmp_qc = get_data_from_db_by_tmp(tmp_table=data_ajbh,tmp_table_name=tmp_table_name,sql=sql,conn=conn_mysql)
    data_ajxx_qc = tmp_qc.copy()
    data_ajxx_qc = data_ajxx_qc.drop_duplicates(subset=['ajbh'])
    data_ajxx_qc.columns = [i.lower()  for i in data_ajxx_qc.columns]
    return data_ajxx_qc


def getWCN_XYR(sjsx='',sjxx=''):
    conn_mysql = db_conn_init("db_mysql_local")
    today = datetime.datetime.now()  # 当前日期时间
    st = today + datetime.timedelta(days = -3,hours=-1,minutes=-1) # 
    et = today + datetime.timedelta(days = 0,hours=0) #
    sjsx = str(st)
    sjxx = str(et)
    print_info(f"查询本地嫌疑人库时间范围:{sjsx}_{sjxx}")
    other_lb = ('1','2')
    sql = f'''
        select aj.ajbh,xyr.zhrq,xyr.zhdw,xyr.userid
        from aj_xyrxx xyr
        inner join aj_jbxx aj on aj.ajbh=xyr.ajbh
        where xyr.zhrq>='{sjsx}' and xyr.zhrq<='{sjxx}' and (aj.ajlb = '盗窃案' or ajlb in  {other_lb}) and aj.slsj is not null
    '''
    print(sql)
    data_source = get_data_from_db(sql=sql,conn=conn_mysql).drop_duplicates(subset=['ajbh',"userid"])
    print_info(f"查询本地嫌疑人库时间范围:{sjsx}-{sjxx},结果数:{data_source.shape[0]}")
    sql = '''
        select distinct ajbh exist_asjbh,userid exist_userid from theme_ry 
        where data_source = '本地嫌疑人库'
    '''
    exist_aj = get_data_from_db(sql=sql,conn=conn_mysql)
    print("库中已存嫌疑人数",exist_aj.shape[0])
    tmp = data_source.merge(exist_aj,left_on=["ajbh","userid"],right_on=["exist_asjbh","exist_userid"],how='left')
    tmp_qc = tmp[pd.isna(tmp["exist_asjbh"])==1].drop(columns=['exist_asjbh',"exist_userid"])
    if tmp_qc.empty:
        print_info(f"查询本地嫌疑人,结果数:{tmp_qc.shape[0]}")
        return pd.DataFrame()
    tmp_qc = tmp_qc[tmp_qc['userid'].apply(check_sfz)]
    if tmp_qc.empty:
        print_info(f"查询本地嫌疑人,结果数:{tmp_qc.shape[0]}")
        return pd.DataFrame()
    tmp_qc["zhrq"] = tmp_qc["zhrq"].apply(lambda x: format_sj(x)[0:10])
    if tmp_qc.empty:
        print_info(f"查询本地嫌疑人,结果数:{tmp_qc.shape[0]}")
        return pd.DataFrame()
    print_info(f"查询本地嫌疑人剩余结果数:{tmp_qc.shape[0]}")
    result_ajxx = GetAJXXFromAJBH(list(set(tmp_qc["ajbh"]))).drop_duplicates(subset=['ajbh'])
    本地库前科未成年 = tmp_qc.merge(result_ajxx,on=["ajbh"]).drop_duplicates(subset=['ajbh',"userid"])
    本地库前科未成年["fasjsx"] = 本地库前科未成年["fasjsx"].apply(lambda x: format_sj(x))
    本地库前科未成年["fasjxx"] = 本地库前科未成年["fasjxx"].apply(lambda x: format_sj(x))
    
    本地库前科未成年["data_source"] = '本地嫌疑人库'
    本地库前科未成年["ryksj"] = 本地库前科未成年["zhrq"].apply(format_sj)
    本地库前科未成年["rztksj"] =  str(datetime.datetime.now())[0:19]
    本地库前科未成年["fh_rksj"] =  str(datetime.datetime.now())[0:19]
    print(本地库前科未成年)
    write2db(本地库前科未成年,"theme_ry",mode='w+',conn=conn_mysql)
    print_info(f"查询本地嫌疑人库时间范围:{sjsx}-{sjxx},结果数:{本地库前科未成年.shape[0]}")
    return 本地库前科未成年

if __name__ == '__main__':
    data = getWCN_XYR()
    print_info("数据更新完毕")
    