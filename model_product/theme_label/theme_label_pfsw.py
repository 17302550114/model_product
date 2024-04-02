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
    list_req_gj_type = ['网吧上网','']
    data_to_pfkf =  data_gj[data_gj['gj_type'].apply(lambda x: x in list_req_gj_type)]
    data_pfkf = data_to_pfkf.groupby(by=['userid']).agg({'gj_xq':lambda x: len(set(x))}).reset_index()
    data_pfkf = data_pfkf.sort_values(by=['gj_xq'],ascending=False).rename(columns={"gj_xq":"swcs"})
    thred = np.percentile(data_pfkf['swcs'],(80))
    result_pfrz =  data_pfkf[data_pfkf.apply(lambda x:  x.swcs>thred ,axis=1)]
    result_pfrz["label"] = "频繁上网"
    result_pfrz["rztksj"] = str(datetime.datetime.now())[0:19]
    result_pfrz.columns = [i.lower() for i in result_pfrz.columns]
    write2db(result_pfrz,'theme_label_pfsw',mode='w',conn=conn_mysql)
    print_info(f"分析频繁上网完成,结果数:{result_pfrz.shape[0]}")
