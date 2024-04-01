
import os
import sys
import datetime
import pandas as pd

sys.path.append(os.getcwd())
print("当前工作路径",os.getcwd())
from tools.other_tool import *
from model_op.op_track import *
from sql_conn import *
from multiprocessing import *

conn_mysql = db_conn_init("db_mysql_local")
now = datetime.datetime.now()
model_path = '标签相关/'
file_path =   PROJECT_BASE_PATH  + '/model_product/logs/' + model_path
my_logger(file_path,"更新标签_昼伏夜出_夜间出行.log")



if __name__ == '__main__':
    today = datetime.datetime.now()  # 当前日期时间
    sjsx = str(today + datetime.timedelta(days = -30,hours=-0)) # 
    sql = f'''
        select distinct userid,gj_sj,gj_location,gj_type
        from theme_gj  where task_id = '000' and gj_sj>'{sjsx}'
    '''
    data_gj = get_data_from_db(sql=sql,conn=conn_mysql)
    list_req_gj_type = ['人像轨迹','']
    data_to_zfyc =  data_gj[data_gj['gj_type'].apply(lambda x: x in list_req_gj_type)]
    list_  = []
    for name, group in data_to_zfyc.groupby(by=["userid"]):
        list_gj_sj = list(group.gj_sj)
        (cxts,yjcxts) =  opTrackZFYC(list_gj_sj,)
        dict_ = {}
        dict_["userid"] = name
        dict_["cxts"] = cxts
        dict_["zfycts"] = yjcxts
        dict_['zfyc_rate'] = round(yjcxts/cxts,1)
        list_.append(dict_)
    data_zfyc = pd.DataFrame(list_)

    thred = np.percentile(data_zfyc['zfycts'],(90))
    result_zfyc =  data_zfyc[data_zfyc.apply(lambda x:  x.zfycts>thred and x.zfyc_rate>=0.5,axis=1)]
    result_zfyc["label"] = "昼伏夜出"
    result_zfyc["rztksj"] = str(datetime.datetime.now())[0:19]
    result_zfyc.columns = [i.lower() for i in result_zfyc.columns]
    write2db(result_zfyc,'theme_label_zfyc',mode='w',conn=conn_mysql)
    print_info(f"分析昼伏夜出完成,结果数:{result_zfyc.shape[0]}")

    list_  = []
    for name, group in data_to_zfyc.groupby(by=["userid"]):
        list_gj_sj = list(group.gj_sj)
        (cxts,yjcxts) =  opTrackYJCX(list_gj_sj,)
        dict_ = {}
        dict_["userid"] = name
        dict_["cxts"] = cxts
        dict_["yjcxts"] = yjcxts
        dict_['yjcx_rate'] = round(yjcxts/cxts,1)
        list_.append(dict_)
    data_yjcx = pd.DataFrame(list_)
    thred = np.percentile(data_yjcx['yjcxts'],(90))
    result_yjcx =  data_yjcx[data_yjcx.apply(lambda x:  x.yjcxts>thred and x.yjcx_rate>=0.5,axis=1)]
    result_yjcx["label"] = "夜间出行"
    result_yjcx["rztksj"] = str(datetime.datetime.now())[0:19]
    write2db(result_yjcx,'theme_label_yjcx',mode='w',conn=conn_mysql)
    print_info(f"分析夜间出行完成,结果数:{result_yjcx.shape[0]}")