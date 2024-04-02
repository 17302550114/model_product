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
    data_to_jc_rel = data_gj.copy()
    data_to_jc_rel.columns = ["rel_" + i for i in data_to_jc_rel.columns]
    data_to_jc_rel = data_to_jc_rel.rename(columns={"rel_gj_location_id":"gj_location_id"})
    result_jc = data_gj.merge(data_to_jc_rel,on=["gj_location_id"])
    result_jc = result_jc[result_jc.apply(lambda x: x.userid!=x.rel_userid,axis=1)]
    if result_jc.empty:
        return pd.DataFrame()

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
        data_gj = get_data_from_db(sql=sql,conn=conn_mysql)
        tmp = df_sfz.merge(data_gj,on=['userid'])
        
    if tmp.empty:
        print("人像轨迹数据为空")
        result = pd.DataFrame(data=[{"gj_type":"人像轨迹","task_id":task_id}])
        return [pd.DataFrame()] * 2
    print(f"有人像轨迹数据{tmp.shape[0]}")
    tmp['gj_sj'] = tmp['passtime'].apply(format_sj)
    tmp['gj_location'] = tmp['device_address']
    tmp['gj_location_id'] = tmp['device_id']
    tmp['gj_xq'] = tmp['gj_location']
    result_rxtx = gen_rel_rxtx(tmp)

    result_rxgj = tmp[['userid','gj_sj','gj_location','gj_xq','gj_location_id','jd','wd']]
    result_rxgj['gj_type']='人像轨迹'
    # 计算人像同行
    print_info(f"旅馆住宿轨迹数:{result_rxgj.shape[0]}")
    print_info(f"旅馆同住轨迹数:{result_rxtx.shape[0]}")
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
    sjsx = format_sj(sjsx)
    sjxx = format_sj(sjxx)
    df_sfz = pd.DataFrame(data=list_sfz,columns=['userid'])
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
        print("旅馆轨迹数据为空")
        result = pd.DataFrame(data=[{"gj_type":"人像轨迹","task_id":task_id}])
        return [pd.DataFrame(),pd.DataFrame()]
    print(f"有旅馆住宿轨迹数据{tmp.shape[0]}")
    tmp['gj_sj'] = tmp['checkintime'].apply(format_sj)
    tmp['gj_sj_r'] = tmp['checkouttime'].apply(format_sj)
    tmp['gj_location'] = tmp['device_address']
    tmp['gj_location_id'] = tmp['device_id']
    tmp['gj_xq'] = tmp.apply(lambda x: f"入住时间:{x.checkintime},退房时间:{x.checkouttime},房号:{x.roomnum}",axis=1 )
    result_lgtz = gen_rel_lgtz(tmp)
    result_gj = tmp[['userid','gj_sj','gj_sj_r','gj_location','gj_xq','gj_location_id','jd','wd']]
    result_gj['gj_type']='旅馆住宿'
    print_info(f"旅馆住宿轨迹数:{result_gj.shape[0]}")
    print_info(f"旅馆同住轨迹数:{result_lgtz.shape[0]}")
    return [result_gj,result_lgtz]

def get_gj_tl(list_sfz,sjsx,sjxx,mode='query',task_id='000'):
    print(sjsx,sjxx)
    sjsx = format_sj(sjsx)[0:10]
    sjxx = format_sj(sjxx)[0:10]
    df_sfz = pd.DataFrame(data=list_sfz,columns=['userid'])
    tmp = pd.DataFrame()
    if mode=='query':
        tmp_table_name = "zdry_gj_tl_tmp"+ str(np.random.random())[2:10]
        sql_tl_dp=f'''
            SELECT gj.userid,gj.cc,gj.fcrq,
            gj.cfz,gj.ddz,gj.cxhm,gj.zwh
            from {tmp_table_name} ry
            inner join gj_tlcx gj  on gj.userid = ry.userid
            where fcrq >= '{sjsx}' and fcrq<='{sjxx}'
        '''
        tmp = get_data_from_db_by_tmp(tmp_table=df_sfz,tmp_table_name=tmp_table_name,sql=sql_tl_dp,conn=conn_mysql)
    if mode == 'monitor':
        sql = f'''
            SELECT gj.userid,gj.cc,gj.fcrq,
            gj.cfz,gj.ddz,gj.cxhm,gj.zwh
            from gj_tlcx gj
            where gj.fcrq >= '{sjsx}' and gj.fcrq<='{sjxx}'
        '''
        data_gj = get_data_from_db(sql=sql,conn=conn_mysql)
        tmp = df_sfz.merge(data_gj,on=['userid'])
        
    if tmp.empty:
        print("铁路轨迹数据为空")
        result = pd.DataFrame(data=[{"gj_type":"铁路轨迹","task_id":task_id}])
        return [pd.DataFrame(),pd.DataFrame()]
    tmp['gj_sj'] = tmp['fcrq'].apply(lambda x: format_sj(x)[0:10] )
    tmp['gj_location'] = tmp['cfz']
    tmp['gj_location_r'] = tmp['ddz']
    tmp['gj_location_id'] = ''
    tmp['gj_xq'] = tmp.apply(lambda x: str(x.cxhm) + '_' +str(x.zwh) ,axis=1)
    result_gj_tl = tmp[['userid','gj_sj','gj_location','gj_xq','gj_location_id','gj_location_r',]]
    result_gj_tl['gj_type']='铁路出行'
    # 计算铁路同行
    result_rxtx = gen_rel_tltx(tmp)
    print_info(f"铁路出行轨迹数:{result_gj_tl.shape[0]}",)
    print_info(f"铁路同行轨迹数:{result_rxtx.shape[0]}",)
    return [result_gj_tl,result_rxtx]

def gen_rel_tltx(data_gj):
    data_to_jc_rel = data_gj.copy()
    # 计算两种关系：同车厢出行；相邻座位（座位号相差小于2）出行 
    df_tl_tx_tcx = pd.DataFrame()
    df_tl_tx_xlzw = pd.DataFrame()
    for i in tqdm(data_to_jc_rel.itertuples(),total=data_to_jc_rel.shape[0],postfix='铁路同行'):
        quer_userid  =  i.userid
        zwh = i.zwh
        if pd.isna(i.cxhm):
            continue
        condition = f"FCRQ='{i.fcrq}' and cc='{i.cc}' and cfz='{i.cfz}' and ddz ='{i.ddz}' and CXHM='{i.cxhm}'"
        sql = f'''
            select userid,fcrq,cc,cfz,ddz,cxhm,zwh from gj_tlcx where {condition}
        '''
        tmp_tx = get_data_from_db(sql=sql,conn=conn_mysql)
        if tmp_tx.empty:
            continue
        tmp_tx = tmp_tx[tmp_tx.apply(lambda x: x.userid != quer_userid,axis=1)]
        tmp_tx["quer_userid"] = quer_userid
        tmp_tx["rel_sj"] = i.fcrq
        tmp_tx["rel_location"] = tmp_tx["cfz"]
        tmp_tx["rel_location_r"] = tmp_tx["ddz"]
        tmp_tx["rel_type"] = "铁路同行_同车厢"
        tmp_tx['rel_userid'] = tmp_tx['userid']
        tmp_tx['rel_xq'] = tmp_tx.apply(lambda x: str(x.cxhm)+'_' + str(zwh) + '_' +str(x.zwh) ,axis=1)
        df_tl_tx_tcx = df_tl_tx_tcx.append(tmp_tx)
        def get_zwhm_limit(x1,x2):
            zwhm_1 = re.findall("\d+",x1)
            zhhm_2 = re.findall('\d+',x2)
            if zwhm_1==[] or zhhm_2==[]:
                return False
            if abs(int(zwhm_1[0])-int(zhhm_2[0]))<=1:
                    return True
            return False
        tmp_tx_xlzw = tmp_tx[tmp_tx.apply(lambda x:  get_zwhm_limit(zwh,x.zwh),axis=1)]
        tmp_tx_xlzw["rel_type"] = "铁路同行_相邻座位"
        df_tl_tx_xlzw  = df_tl_tx_xlzw.append(tmp_tx_xlzw)
    df_tl_tx = df_tl_tx_tcx.append(df_tl_tx_xlzw)
    result_tl_tx = df_tl_tx[["quer_userid",'rel_userid','rel_sj','rel_type','rel_location','rel_location_r','rel_xq']]
    return result_tl_tx

def get_rel_wbsw(data_gj,time_delta=60):
    # 同网吧上网:上下机时间有交叉，且上机时间相差不超过1小时，下机时间相差不超过1小时
    result_t_lgs = pd.DataFrame()
    for i in data_gj.itertuples():
        quer_sjsj = str(i.sjsj)
        quer_xjsj = str(i.xjsj)
        quer_userid = i.userid
        gj_location_id = i.gj_location_id
        jqh= i.jqh
        condition = f" xjsj>='{quer_sjsj}' and sjsj<='{quer_xjsj}'  and wbbh='{gj_location_id}' and userid != '{quer_userid}'"
        sql =f'''
            SELECT gj.userid,gj.wbbh device_id,gj.sjsj,gj.xjsj,gj.jqh,
            device.device_address,device.jd,device.wd from gj_wbsw gj 
            left join device_jbxx device on device.device_id = gj.wbbh 
            where {condition}
        '''
        tmp = get_data_from_db(sql=sql,conn=conn_mysql)
        if tmp.empty:
            continue
        tmp['quer_userid'] = quer_userid
        tmp["rel_userid"] = tmp["userid"]
        tmp["rel_sj"] = tmp["sjsj"].apply(format_sj)
        tmp["rel_sj_r"] = tmp["xjsj"].apply(format_sj)
        tmp["rel_location"] = tmp["device_address"]
        tmp["rel_location_id"] = tmp["device_id"]
        tmp['rel_xq'] = tmp["jqh"].apply(lambda x: jqh + '_' + x )
        tmp["rel_type"] = '同网吧上网'
        tmp = tmp[tmp.apply(lambda x: x.quer_userid!=x.rel_userid,axis=1)]
        # 上下机时间相差不超过1小时
        tmp["dif_sj_days"] = tmp["sjsj"].apply(lambda x: (parser.parse(x)-parser.parse(quer_sjsj)).days)
        tmp["dif_sj_sec"] = tmp["sjsj"].apply(lambda x: (parser.parse(x)-parser.parse(quer_sjsj)).seconds)
        tmp["dif_sj_min"] = tmp.apply(lambda x: abs(x.dif_sj_days*24*3600 + x.dif_sj_sec)/60,axis=1)

        tmp["dif_xj_days"] = tmp["xjsj"].apply(lambda x: (parser.parse(str(x))-parser.parse(quer_xjsj)).days)
        tmp["dif_xj_sec"] = tmp["xjsj"].apply(lambda x: (parser.parse(str(x))-parser.parse(quer_xjsj)).seconds)
        tmp["dif_xj_min"] = tmp.apply(lambda x: abs(x.dif_xj_days*24*3600 + x.dif_xj_sec)/60,axis=1)
        tmp = tmp[tmp.apply(lambda x:  abs(x.dif_sj_min)<30 and  abs(x.dif_xj_min)<30,axis=1 )]
        if tmp.empty:
            continue
        result_t_lg = tmp[["quer_userid","rel_userid","rel_sj","rel_location","rel_xq",'rel_type','rel_location_id']]
        result_t_lgs = result_t_lgs.append(result_t_lg)
    return result_t_lgs

def get_gj_wbsw(list_sfz,sjsx,sjxx,mode='query',task_id='000'):
    print(sjsx,sjxx)
    sjsx = format_sj(sjsx)
    sjxx = format_sj(sjxx)
    df_sfz = pd.DataFrame(data=list_sfz,columns=['userid'])
    tmp = pd.DataFrame()
    if mode=='query':
        tmp_table_name = "zdry_gj_tmp"+ str(np.random.random())[2:10]
        sql_tl_dp=f'''
            SELECT gj.userid,gj.wbbh device_id,gj.sjsj,gj.xjsj,gj.jqh,
            device.device_address,device.jd,device.wd
            from {tmp_table_name} ry
            inner join gj_wbsw gj  on gj.userid = ry.userid
            left join device_jbxx device on device.device_id = gj.wbbh
            where sjsj >= '{sjsx}' and xjsj<='{sjxx}'
        '''
        tmp = get_data_from_db_by_tmp(tmp_table=df_sfz,tmp_table_name=tmp_table_name,sql=sql_tl_dp,conn=conn_mysql)
    if mode == 'monitor':
        sql = f'''
            SELECT gj.userid,gj.wbbh device_id,gj.sjsj,gj.xjsj,gj.jqh,
            device.device_address,device.jd,device.wd
            from gj_wbsw gj 
            left join device_jbxx device on device.device_id = gj.wbbh
            where gj.rksj >= '{sjsx}' and gj.rksj<='{sjxx}'
        '''
        data_gj = get_data_from_db(sql=sql,conn=conn_mysql)
        tmp = df_sfz.merge(data_gj,on=['userid'])
    if tmp.empty:
        print("网吧上网轨迹数据为空")
        return [pd.DataFrame(),pd.DataFrame()]
    print_info(f"网吧上网轨迹数据{tmp.shape[0]}")
    tmp['gj_sj'] = tmp['sjsj'].apply(format_sj)
    tmp['gj_sj_r'] = tmp['xjsj'].apply(format_sj)
    tmp['gj_location'] = tmp['device_address']
    tmp['gj_location_id'] = tmp['device_id']
    tmp['gj_xq'] = tmp["jqh"]
    result_lgtz = get_rel_wbsw(tmp)
    print_info(f"网吧同上网轨迹数据{result_lgtz.shape[0]}")
    result_gj = tmp[['userid','gj_sj','gj_sj_r','gj_location','gj_xq','gj_location_id','jd','wd']]
    result_gj['gj_type']='网吧上网'
    return [result_gj,result_lgtz]

if __name__ == '__main__':
    sql = '''
        select distinct userid from theme_ry
    '''
    data_rybh = get_data_from_db(sql=sql,conn=conn_mysql)
    list_userid = list(set(data_rybh["userid"]))
    today = datetime.datetime.now()  # 当前日期时间
    sjsx = str(today + datetime.timedelta(days = -0,hours=-1)) # 
    sjxx = str(today + datetime.timedelta(days = -0,hours=0)) #
    task_id = '000'
    mode = 'monitor'
    print_info(f"重点人员轨迹查询：开始时间:{str(datetime.datetime.now())},查询人数:{len(list_userid)},查询轨迹时间范围:{sjsx}-{sjxx}")
    ceil = 1000
    df_result_gj = pd.DataFrame()
    df_result_rel = pd.DataFrame()
    for index,list_sfz in tqdm(enumerate(block_list(list_userid,n=ceil)),total = len(list_userid)/ceil):
        # 并行分析
        pool = Pool(10)
        result_rx = pool.apply_async(func=get_gj_rx,args=(list_sfz,sjsx,sjxx,mode), error_callback=error)
        result_lg = pool.apply_async(func=get_gj_lg,args=(list_sfz,sjsx,sjxx,mode), error_callback=error)
        result_tl = pool.apply_async(func=get_gj_tl,args=(list_sfz,sjsx,sjxx,mode), error_callback=error)
        result_wb = pool.apply_async(func=get_gj_wbsw,args=(list_sfz,sjsx,sjxx,mode), error_callback=error)

        pool.close()
        pool.join()
        dict_gj = {
            "result_rx":result_rx,
            "result_lg":result_lg,
            'result_tl':result_tl,
            "result_wb":result_wb
          }
        list_result = [dict_gj.get(i).get()[0] for i in dict_gj.keys() if is_Success(dict_gj,i)]
        result_all_gj = reduce(lambda x,y:x.append(y),list_result)
        df_result_gj = df_result_gj.append(result_all_gj)
        list_result_rel = [dict_gj.get(i).get()[1] for i in dict_gj.keys() if is_Success(dict_gj,i)]
        result_all_rel = reduce(lambda x,y:x.append(y),list_result_rel)
        df_result_rel = df_result_rel.append(result_all_rel)
    print("所有轨迹结果条数",df_result_gj.shape[0])
    print("所有关系结果",df_result_rel.shape[0])
    df_result_gj["task_id"]=task_id
    df_result_gj["rksj"]= str(datetime.datetime.now())[0:19]
    write2db(df_result_gj,'theme_gj',mode='w+',conn=conn_mysql)
    print_info(f"重点人员轨迹查询：结束时间:{str(datetime.datetime.now())},结果数:{df_result_gj.shape[0]}")
    df_result_rel["task_id"]=task_id
    df_result_rel["rksj"]= str(datetime.datetime.now())[0:19]
    conn_mysql = db_conn_init("db_mysql_local")
    write2db(df_result_rel,'theme_gj_rel',mode='w+',conn=conn_mysql)
    print_info(f"重点人员轨迹关系查询：结束时间:{str(datetime.datetime.now())},结果数:{df_result_rel.shape[0]}")
    conn_mysql.dispose()