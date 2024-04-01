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
sys.path.append('/home/zhxz_model')
# os.chdir(os.path.dirname(os.path.abspath(__file__)))

from sql_conn import *
from multiprocessing import *
from tools.other_tool import *
def error(error):
    print(error)

conn_mysql = db_conn_init("db_mysql_local")
now = datetime.datetime.now()
model_path = '人员相关/'
file_path =   '/home/zhxz_model/model_product/logs/' + model_path
my_logger(file_path,"更新关注人员一人一档.log")
def gen_qksl(df):
    数据来源 = df.groupby(by=['userid']).agg({"data_source":lambda x:",".join(set(x)),
                                                    "zhrq":lambda x:max([i for i in set(x) if pd.isna(i)!=1 and str(i)<str(now) ] + [""])
                                                }).reset_index()
    数据来源.columns = ["userid","data_sources","max_zhrq"]
    result_1 = 数据来源.merge(data_rybh[['userid',"zhrq","zhdw","ajlb"]],left_on=["userid","max_zhrq"],right_on=["userid","zhrq"],how='left').drop_duplicates()
    result_2 =  result_1.drop_duplicates(subset=["userid","data_sources","max_zhrq"]).drop(columns=['zhrq'])
    result_2.columns = [i.lower() for i in result_2.columns]
    data_to_sl = data_rybh[data_rybh["data_source"].apply(lambda x: str(x)=='本地嫌疑人库')]
    前科数量 = data_to_sl.groupby(by=["userid"]).agg({"ajbh":lambda x: len(set(x))}).reset_index()
    前科数量.columns = ["userid","qksl"]
    result_3 = result_2.merge(前科数量,how='left')
    result_3["qksl"] = result_3["qksl"].fillna(0)
    result_3["qksl"] = result_3.apply(lambda x : x.qksl+1 if "湖州外协" in x.data_sources else x.qksl,axis=1)
    result_3["qksl"] = result_3.apply(lambda x : 1 if x.qksl==0 else x.qksl,axis=1)
    result_3 = result_3.astype('str')
    return result_3

def gen_ry_jbxx(df):
    
    tmp_table_name = 'df2wcnjbxx' + str(np.random.random())[2:10]
    sql=f'''
        SELECT sfz.userid,name xm,ry.rylb,xzzxz,fwcs,phone,zw,plateno from {tmp_table_name} sfz
        inner join ry_jbxx ry on sfz.userid = ry.USERID
        '''
    df_ry_jbxx = get_data_from_db_by_tmp(tmp_table=df[["userid"]],tmp_table_name=tmp_table_name,sql=sql,conn=conn_mysql)
    df_ry_jbxx.columns = [i.lower() for i in df_ry_jbxx.columns]
    df_ry_jbxx = df_ry_jbxx.drop_duplicates(subset=['userid'])
    ry_jbxx = df_ry_jbxx
    ry_jbxx[["privince","city",'area']] = ry_jbxx.apply(lambda x: get_area_div(str(x.userid)[0:6]),result_type='expand',axis=1)
    ry_jbxx['city'] = ry_jbxx.progress_apply(lambda x: x.privince if str(x.city)=='nan' or pd.isna(x.city) else x.city,axis=1)
    ry_jbxx["hjdz"] = ry_jbxx.progress_apply(lambda x:  str(x.city) +   str(''.join([ i for i in list(str(x.area)) if i not in list(str(x.privince))])),axis=1  )
    ry_jbxx["hjdz"] = ry_jbxx["hjdz"].progress_apply(lambda x: x.replace('nan',''))
    ry_jbxx['hjdz'] = ry_jbxx.apply(lambda x:x.privince if pd.isna(x.hjdz) or x.hjdz=='None' else x.hjdz,axis=1)
    ry_jbxx["age"] = ry_jbxx["userid"].apply(judgeAge)
    ry_jbxx["age"] = ry_jbxx["age"].replace(0x00,999)
    ry_jbxx["xb"] = ry_jbxx['userid'].apply(lambda x:  '男'  if int(x[16:17])%2==1 else '女')
    return ry_jbxx


if __name__ == '__main__':
    sql = '''
        select * from theme_ry
    '''
    data_rybh = get_data_from_db(sql=sql,conn=conn_mysql)
    result_qksl = gen_qksl(data_rybh)
    result_ry_jbxx = gen_ry_jbxx(data_rybh)
    result_1 = result_qksl.merge(result_ry_jbxx,on=['userid'],how='left')

    # 关联标签
    sql = '''
        select userid,txrs label_score,'人像同行' as label from theme_label_rxtx
    '''
    data_label_rxtx = get_data_from_db(sql=sql,conn=conn_mysql)

    sql = '''
        select userid,tzrs label_score,'旅馆同住' as label from theme_label_lgtz
    '''
    data_label_lgtz = get_data_from_db(sql=sql,conn=conn_mysql)

    sql = '''
        select userid,1 label_score,'昼伏夜出' as label from theme_label_zfyc
    '''
    data_label_zfyc = get_data_from_db(sql=sql,conn=conn_mysql)

    
    sql = '''
        select userid,1 label_score,'夜间出行' as label from theme_label_yjcx
    '''
    data_label_yjcx = get_data_from_db(sql=sql,conn=conn_mysql)


    list_result_label = [data_label_rxtx,data_label_lgtz,data_label_zfyc,data_label_yjcx]
    result_label = reduce(lambda x,y:x.append(y),list_result_label)

    result_label["label_score"] = result_label["label_score"].astype('float')
    result_label = result_label.groupby(by=['userid']).agg({"label_score":sum,
                                             "label":lambda x: ','.join(set(x))
                                            }).reset_index()
    result_label = result_label.sort_values(by=['label_score'])
    result_label['label_score'] = result_label['label_score'].apply(lambda x:round(x,2))
    result_2 = result_1.merge(result_label,how='left')
    result_2["label_score"] = result_2["label_score"].fillna(0)

    result_2["gxsj"] = str(datetime.datetime.now())[0:19]
    write2db(result_2,'result_yryd',mode='w',conn=conn_mysql)
    print_info(f"更新一人一档完成,人数：{result_2.shape[0]}")
