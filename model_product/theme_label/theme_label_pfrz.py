import os
import re
import sys
import random
import datetime
import pandas as pd
import psycopg2
from dateutil import parser
from pathlib import Path
from functools import reduce
sys.path.append(os.getcwd())
from sql_conn import *
from multiprocessing import *
from tools.other_tool import *

conn_mysql = db_conn_init("db_mysql_local")
now = datetime.datetime.now()

model_path = '标签相关/'
file_path =  PROJECT_BASE_PATH +  '/model_product/logs/' + model_path
my_logger(file_path,"更新标签_频繁入住&夜间入住.log")

if __name__ == '__main__':
    today = datetime.datetime.now()  # 当前日期时间
    sjsx = str(today + datetime.timedelta(days = -30,hours=-0)) # 
    sql = f'''
        select distinct userid,gj_sj,gj_location,gj_type,gj_xq
        from theme_gj  where task_id = '000' and gj_sj>'{sjsx}'
    '''
    data_gj = get_data_from_db(sql=sql,conn=conn_mysql)
    list_req_gj_type = ['旅馆住宿','']
    data_to_pfkf =  data_gj[data_gj['gj_type'].apply(lambda x: x in list_req_gj_type)]
    data_pfkf = data_to_pfkf.groupby(by=['userid']).agg({'gj_xq':lambda x: len(set(x))}).reset_index()
    data_pfkf = data_pfkf.sort_values(by=['gj_xq'],ascending=False).rename(columns={"gj_xq":"rzcs"})
    pencent = 90
    thred = np.percentile(data_pfkf['rzcs'],(pencent))
    result_pfrz =  data_pfkf[data_pfkf.apply(lambda x:  x.rzcs>thred ,axis=1)]
    result_pfrz["label"] = "频繁入住"
    result_pfrz["label_score"] = 1
    result_pfrz["label_rule"] = f"在关注人员中取{100-pencent}%最高入住次数人员,标签分数为1" 
    result_pfrz["rztksj"] = str(datetime.datetime.now())[0:19]
    result_pfrz.columns = [i.lower() for i in result_pfrz.columns]
    write2db(result_pfrz,'theme_label_pfrz',mode='w',conn=conn_mysql)
    print_info(f"分析频繁入住完成,结果数:{result_pfrz.shape[0]}")


    # 夜间入住：入住时间在23时至第二天凌晨3时
    data_to_yjrz = data_to_pfkf[data_to_pfkf.apply(lambda x: parser.parse(x.gj_sj).hour  not in list(range(3,23)),axis=1)]
    data_yjrz = data_to_yjrz.groupby(by=['userid']).agg({'gj_xq':lambda x: len(set(x))}).reset_index()
    data_yjrz = data_yjrz.sort_values(by=['gj_xq'],ascending=False).rename(columns={"gj_xq":"rzcs"})
    thred = np.percentile(data_yjrz['rzcs'],(80))
    result_yjrz =  data_yjrz[data_yjrz.apply(lambda x:  x.rzcs>thred ,axis=1)]
    result_yjrz["label"] = "夜间入住"
    result_yjrz["label_rule"] = "夜间23时至第二天凌晨3时之间入住,标签分数为1"    
    result_yjrz["rztksj"] = str(datetime.datetime.now())[0:19]
    result_yjrz.columns = [i.lower() for i in result_yjrz.columns]
    write2db(result_yjrz,'theme_label_yjrz',mode='w',conn=conn_mysql)
    print_info(f"分析夜间入住完成,结果数:{result_yjrz.shape[0]}")
