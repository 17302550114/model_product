import os
import sys
import datetime
import pandas as pd

sys.path.append(os.getcwd())
from tools.other_tool import *

from sql_conn import *
from multiprocessing import *

conn_mysql = db_conn_init("db_mysql_local")
now = datetime.datetime.now()
model_path = '标签相关/'
file_path =   PROJECT_BASE_PATH  + '/model_product/logs/' + model_path
my_logger(file_path,"更新标签_人像同行.log")

if __name__ == '__main__':
    sql = '''
        select distinct rel.quer_userid,rel.rel_userid,rel.rel_sj  
        from theme_gj_rel rel 
        inner join theme_ry ry on rel.rel_userid = ry.userid
        where rel.rel_type = '人像同行'
    '''
    data_rel_rxtx = get_data_from_db(sql=sql,conn=conn_mysql)

    data_rel_rxtx = data_rel_rxtx.rename(columns={"quer_userid":"userid"})
    同行人数 = data_rel_rxtx.groupby(by=["userid"]).agg({"rel_userid":lambda x: len(set(x))}).reset_index().sort_values(by=['rel_userid'])
    同行人数.columns=["userid","txrs"]
    result_lable_rxtx = data_rel_rxtx.merge(同行人数,on=["userid"])
    result_lable_rxtx["label_rule"] = "与未成年人前科人像同行，标签分数为同行人数"
    result_lable_rxtx["rztksj"] = str(datetime.datetime.now())[0:19]
    result_lable_rxtx.columns = [i.lower() for i in result_lable_rxtx.columns]
    write2db(result_lable_rxtx,'theme_label_rxtx',mode='w',conn=conn_mysql)
    print_info(f"分析人像同行完成,结果数:{result_lable_rxtx.shape[0]}")