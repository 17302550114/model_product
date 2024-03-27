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
from functools import reduce
sys.path.append(os.getcwd())

from sql_conn import *
from multiprocessing import *
from tools.other_tool import *

conn_mysql = db_conn_init("db_mysql_local")
now = datetime.datetime.now()
model_path = '轨迹相关/'
file_path =  PROJECT_BASE_PATH +  '/model_product/logs/' + model_path
my_logger(file_path,"更新人员全量轨迹.log")
def error(error):
    print(error)
    pass
def is_Success(dict_,x):
    df_ = dict_.get(x)
    try:
        result = df_.successful()
    except Exception as e:
        log_info = f"{x},结果数据不存在,检查结果字典..."
        print(log_info)
        logging.info(log_info)
        result = False
    return result

def gen_rel_rxtx(data_gj):
    print(data_gj)
    data_to_jc_rel = data_gj.copy()
    data_to_jc_rel.columns = ["rel_" + i for i in data_to_jc_rel.columns]
    data_to_jc_rel = data_to_jc_rel.rename(columns={"rel_gj_location_id":"gj_location_id"})
    result_jc = data_gj.merge(data_to_jc_rel,on=["gj_location_id"])
    result_jc = result_jc[result_jc.apply(lambda x: x.userid!=x.rel_userid,axis=1)]
    if result_jc.empty:
        return pd.DataFrame()
    print(result_jc)

    result_jc["dif_days"] = result_jc.progress_apply(lambda x:  (parser.parse(x.gj_sj)- parser.parse(x.rel_gj_sj)).days,axis=1)
    result_jc["dif_seconds"] = result_jc.progress_apply(lambda x:  round((parser.parse(x.gj_sj)- parser.parse(x.rel_gj_sj)).seconds),axis=1)
    result_jc["dif_seconds"] = result_jc.progress_apply(lambda x:  abs(x.dif_days*24*3600 + x.dif_seconds),axis=1)
    result_jc = result_jc[result_jc.progress_apply(lambda x: x.dif_seconds<=60,axis=1)]
    print_info(f"人像同行交叉结果数:{result_jc.shape[0]}")
    result_jc['quer_userid'] = result_jc['userid']
    result_jc["rel_sj"] = result_jc["gj_sj"]
    result_jc["rel_location"] = result_jc["gj_location"]
    result_jc["rel_location_id"] = result_jc["gj_location_id"]
    result_jc["rel_xq"] = result_jc["dif_seconds"]
    result_rxtx = result_jc[["quer_userid",'rel_userid','rel_sj','rel_location',"rel_location_id",'rel_xq']]
    result_rxtx["rel_type"] = '人像同行'
    return result_rxtx


def get_gj_rx(list_sfz,sjsx,sjxx,mode='query',task_id='000'):
    conn_mysql = db_conn_init("db_mysql_local")
    sjsx = format_sj(sjsx)
    sjxx = format_sj(sjxx)
    df_sfz = pd.DataFrame(data=list_sfz,columns=['userid'])
    print("查询模式",mode)
    tmp = pd.DataFrame()
    if mode=='query':
        tmp_table_name = "zdry_gj_tmp"+ str(np.random.random())[2:10]
        sql_tl_dp=f'''
            SELECT ry.userid,gj.device_id,gj.passtime,
            device.device_address,device.jd,device.wd
            from {tmp_table_name} ry
            inner join gj_rxgj gj  on gj.userid = ry.userid 
            left join device_jbxx device on device.device_id = gj.device_id
            where passtime >= '{sjsx}' and passtime<='{sjxx}'
        '''
        tmp = get_data_from_db_by_tmp(tmp_table=df_sfz,tmp_table_name=tmp_table_name,sql=sql_tl_dp,conn=conn_mysql)
    if mode == 'monitor':
        sql = f'''
            SELECT gj.userid,gj.device_id,gj.passtime,
            device.device_address,device.jd,device.wd
            from gj_rxgj gj 
            left join device_jbxx device on device.device_id = gj.device_id
            where gj.rksj >= '{sjsx}' and gj.rksj<='{sjxx}'
        '''
        print(sql)
        data_gj = get_data_from_db(sql=sql,conn=conn_mysql)
        print(f"时间范围内人像轨迹数：{data_gj.shape[0]}")
        tmp = df_sfz.merge(data_gj,on=['userid'])
        
    if tmp.empty:
        print("人像轨迹数据为空")
        result = pd.DataFrame(data=[{"gj_type":"人像轨迹","task_id":task_id}])
        return pd.DataFrame()
    print(f"有人像轨迹数据{tmp.shape[0]}")
    tmp['gj_sj'] = tmp['passtime'].apply(format_sj)
    tmp['gj_location'] = tmp['device_address']
    tmp['gj_location_id'] = tmp['device_id']
    tmp['gj_xq'] = tmp['gj_location']
    result_rxtx = gen_rel_rxtx(tmp)

    result_rxgj = tmp[['userid','gj_sj','gj_location','gj_xq','gj_location_id','jd','wd']]
    result_rxgj['gj_type']='人像轨迹'
    # 计算人像同行
    
    return [result_rxgj,result_rxtx]


def gen_rel_lgtz(data_gj,time_delta=15):
    result_t_lgs = pd.DataFrame()
    for i in data_gj.itertuples():
        sj = i.gj_sj
        quer_userid = i.userid
        gj_location_id = i.gj_location_id
        roomnum = i.roomnum
        sjsx = str(parser.parse(sj)+datetime.timedelta(minutes=-time_delta))
        sjxx = str(parser.parse(sj)+datetime.timedelta(minutes=time_delta))
        condition = f"checkintime>='{sjsx}' and checkintime<='{sjxx}' and corpid='{gj_location_id}'"
        sql =f'''
            SELECT gj.userid,gj.corpid device_id,gj.checkintime,gj.checkouttime,gj.roomnum,
            device.device_address,device.jd,device.wd from gj_lgzs gj 
            left join device_jbxx device on device.device_id = gj.corpid 
            where {condition}
        '''
        tmp = get_data_from_db(sql=sql,conn=conn_mysql)
        if tmp.empty:
            continue
        tmp['quer_userid'] = quer_userid
        tmp["rel_userid"] = tmp["userid"]
        tmp["rel_sj"] = tmp["checkintime"].apply(format_sj)
        tmp["rel_sj_r"] = tmp["checkouttime"].apply(format_sj)
        tmp["rel_location"] = tmp["device_address"]
        tmp["rel_location_id"] = tmp["device_id"]
        tmp['rel_xq'] = tmp["roomnum"]
        tmp = tmp[tmp.apply(lambda x: x.quer_userid!=x.rel_userid,axis=1)]
        if tmp.empty:
            continue
        tmp["rel_type"] = tmp.apply(lambda x: '旅馆同住' if str(x.roomnum)==roomnum else '同旅馆',axis=1 )
        result_t_lg = tmp[["quer_userid","rel_userid","rel_sj","rel_sj_r","rel_location","rel_xq",'rel_type']]
        result_t_lgs = result_t_lgs.append(result_t_lg)
    return result_t_lgs


def get_gj_lg(list_sfz,sjsx,sjxx,mode='query',task_id='000'):
    conn_mysql = db_conn_init("db_mysql_local")
    print(sjsx,sjxx)
    sjsx = format_sj(sjsx)
    sjxx = format_sj(sjxx)
    df_sfz = pd.DataFrame(data=list_sfz,columns=['userid'])
    print("查询模式",mode)
    tmp = pd.DataFrame()
    if mode=='query':
        tmp_table_name = "zdry_gj_tmp"+ str(np.random.random())[2:10]
        sql_tl_dp=f'''
            SELECT ry.userid,gj.corpid device_id,gj.checkintime,gj.checkouttime,gj.roomnum,
            device.device_address,device.jd,device.wd
            from {tmp_table_name} ry
            inner join gj_lgzs gj  on gj.userid = ry.userid
            left join device_jbxx device on device.device_id = gj.corpid
            where checkintime >= '{sjsx}' and checkintime<='{sjxx}'
        '''
        tmp = get_data_from_db_by_tmp(tmp_table=df_sfz,tmp_table_name=tmp_table_name,sql=sql_tl_dp,conn=conn_mysql)
    if mode == 'monitor':
        sql = f'''
            SELECT gj.userid,gj.corpid device_id,gj.checkintime,gj.checkouttime,gj.roomnum,
            device.device_address,device.jd,device.wd
            from gj_lgzs gj 
            left join device_jbxx device on device.device_id = gj.corpid
            where gj.rksj >= '{sjsx}' and gj.rksj<='{sjxx}'
        '''
        data_gj = get_data_from_db(sql=sql,conn=conn_mysql)
        tmp = df_sfz.merge(data_gj,on=['userid'])
    if tmp.empty:
        print("人像轨迹数据为空")
        result = pd.DataFrame(data=[{"gj_type":"人像轨迹","task_id":task_id}])
        return pd.DataFrame()
    print(f"有旅馆住宿轨迹数据{tmp.shape[0]}")
    tmp['gj_sj'] = tmp['checkintime'].apply(format_sj)
    tmp['gj_sj_r'] = tmp['checkouttime'].apply(format_sj)
    tmp['gj_location'] = tmp['device_address']
    tmp['gj_location_id'] = tmp['device_id']
    tmp['gj_xq'] = tmp.apply(lambda x: f"入住时间:{x.checkintime},退房时间:{x.checkouttime},房号:{x.roomnum}",axis=1 )
    result_lgtz = gen_rel_lgtz(tmp)
    result_gj = tmp[['userid','gj_sj','gj_sj_r','gj_location','gj_xq','gj_location_id','jd','wd']]
    result_gj['gj_type']='旅馆住宿'
    return result_gj,result_lgtz



if __name__ == '__main__':
    sql = '''
        select distinct userid from theme_ry
    '''
    data_rybh = get_data_from_db(sql=sql,conn=conn_mysql)
    list_userid = list(set(data_rybh["userid"]))
    today = datetime.datetime.now()  # 当前日期时间
    sjsx = str(today + datetime.timedelta(days = -0,hours=-1)) # 
    sjxx = str(today + datetime.timedelta(days = 0,hours=0)) #
    task_id = '000'
    print_info(f"重点人员轨迹查询：开始时间:{str(datetime.datetime.now())},查询人数:{len(list_userid)},查询轨迹时间范围:{sjsx}-{sjxx}")
    ceil = 1000
    df_result_gj = pd.DataFrame()
    df_result_rel = pd.DataFrame()
    for index,list_sfz in tqdm(enumerate(block_list(list_userid,n=ceil)),total = len(list_userid)/ceil):
        # 并行分析
        pool = Pool(10)
        result_rx = pool.apply_async(func=get_gj_rx,args=(list_sfz,sjsx,sjxx,"monitor"), error_callback=error)
        result_lg = pool.apply_async(func=get_gj_lg,args=(list_sfz,sjsx,sjxx,"monitor"), error_callback=error)
        pool.close()
        pool.join()
        dict_gj = {
            "result_rx":result_rx,
            "result_lg":result_lg
          }
        print("数据类型结果字典",dict_gj)
        list_result = [dict_gj.get(i).get()[0] for i in dict_gj.keys() if is_Success(dict_gj,i)]
        result_all_gj = reduce(lambda x,y:x.append(y),list_result)
        df_result_gj = df_result_gj.append(result_all_gj)
        list_result_rel = [dict_gj.get(i).get()[1] for i in dict_gj.keys() if is_Success(dict_gj,i)]
        result_all_rel = reduce(lambda x,y:x.append(y),list_result_rel)
        df_result_rel = df_result_rel.append(result_all_rel)
        print(df_result_gj)
        print(df_result_rel)
    print("分析完成")
    df_result_gj["task_id"]=task_id
    df_result_gj["rksj"]= str(datetime.datetime.now())[0:19]
    write2db(df_result_gj,'theme_gj',mode='w+',conn=conn_mysql)
    print_info(f"重点人员轨迹查询：结束时间:{str(datetime.datetime.now())},结果数:{df_result_gj.shape[0]}")
    df_result_rel["task_id"]=task_id
    df_result_rel["rksj"]= str(datetime.datetime.now())[0:19]
    write2db(df_result_rel,'theme_gj_rel',mode='w+',conn=conn_mysql)
    print_info(f"重点人员轨迹关系查询：结束时间:{str(datetime.datetime.now())},结果数:{df_result_rel.shape[0]}")
    conn_mysql.dispose()