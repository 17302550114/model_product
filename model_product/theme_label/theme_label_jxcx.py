
import os
import sys
import datetime
import pandas as pd
sys.path.append(os.getcwd())
from tools.other_tool import *
from model_op.op_track import *
from sql_conn import *
from multiprocessing import *

conn_mysql = db_conn_init("db_mysql_local")
now = datetime.datetime.now()
model_path = '标签相关/'
file_path =   PROJECT_BASE_PATH  + '/model_product/logs/' + model_path
my_logger(file_path,"更新标签_间歇出现.log")


if __name__ == '__main__':
    today = datetime.datetime.now()  # 当前日期时间
    sjsx = str(today + datetime.timedelta(days = -70,hours=-0)) # 
    sql = f'''
        select distinct userid,gj_sj,gj_location,gj_type,gj_xq
        from theme_gj  where task_id = '000' and gj_sj>'{sjsx}'
    '''
    data_to_jxcx = get_data_from_db(sql=sql,conn=conn_mysql)
    list_  = []
    for name, group in data_to_jxcx.groupby(by=["userid"]):
        list_gj_sj = list(group.gj_sj)
        (cxts_fjq,min_dif_days) =  opTrackJXCX(list_gj_sj,)
        dict_ = {}
        dict_["userid"] = name
        dict_["cxts_fjq"] = cxts_fjq
        dict_["min_dif_days"] = min_dif_days
        list_.append(dict_)
    result_jxcx = pd.DataFrame(list_)
    result_jxcx = result_jxcx[result_jxcx["min_dif_days"]!=0].sort_values(by=['min_dif_days'])
    # thred = np.percentile(result_jxcx['cxts_jq'],(90))
    # result_jxcx =  result_jxcx[result_jxcx.apply(lambda x:  x.cxts_jq>thred ,axis=1)]
    result_jxcx["label"] = "间歇出现"
    result_jxcx["rztksj"] = str(datetime.datetime.now())[0:19]
    result_jxcx.columns = [i.lower() for i in result_jxcx.columns]
    write2db(result_jxcx,'theme_label_jxcx',mode='w',conn=conn_mysql)
    print_info(f"分析间歇出现完成,结果数:{result_jxcx.shape[0]}")