'''
Author: geng 765621013@qq.com
Date: 2024-03-13 09:56:50
LastEditors: geng 765621013@qq.com
LastEditTime: 2024-03-13 11:12:37
FilePath: \study_peoject\zhxz_model\sql_conn.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
"""
140,xzj oracle连接及数据的查找，存储，更新数据操作，每次调用完方法，及时调用 close_conn 方法关闭连接
61,zhxz_mx postgresql连接及数据的查找，存储，更新数据操作，pg库存储，字段名统一使用小写
"""
import os
import sys
import json
import pandas as pd
from sqlalchemy import create_engine, types,MetaData, Table
import pymysql
pymysql.install_as_MySQLdb()
sys.path.append('/home/zhxz_model')
from tools.other_tool import *

with open('/home/zhxz_model/setting.json') as j:
    settings = json.load(j)

def db_conn_init(db_name):
    
    db_setting = settings[db_name]
    prefix = db_setting['prefix']
    host = db_setting['host']
    port = db_setting['port']
    user = db_setting['user']
    password = db_setting['password']
    database = db_setting['database']
    str_conn = f"{prefix}://{user}:{password}@{host}:{port}/{database}"
    conn_db = create_engine(str_conn)
    return conn_db

def get_data_from_db(sql, isClOB=False, conn=None) -> pd.DataFrame:
    """
    :param conn:
    :param sql:查询的完整的sql语句
    :param isClOB:是否包含CLOB类型的数据，defaul=False
    :param conn:数据库连接选择，ora_conn，oracle库，pg_conn，pg库，支持自定义连接，defaul=ora_conn
    :return: 返回df
    """
    if isClOB:
        cur = conn.cursor()
        cur.execute(sql)
        result = cur.fetchall()
        df = pd.DataFrame(
            list(map(lambda x: [i.read() if isinstance(i, cx_Oracle.LOB) else i for i in x],
                        result))).drop_duplicates()
        cur.close()
    else:
        df = pd.read_sql(sql, conn).drop_duplicates()
    return df

def write2db(df, outname, mode, conn=None):
    """把DataFrame数据格式直接存到库里
    :param engine:
    :param df: 输入存储的表
    :param outname: 入库的表名
    :param mode: 存储形式，'w+'追加写数据，'w'覆盖写数据
    :return:
    """
    dict_mode = {"w+":"append",'w':"replace"}
    dtype = {c: types.VARCHAR(2000) for c in df.columns[df.dtypes == 'object'].tolist()}
    df.to_sql(outname, conn, if_exists=dict_mode[mode], index=False, index_label=None, dtype=None)

def get_data_from_db_by_tmp(tmp_table='',tmp_table_name='',sql='',conn=''):
    # 1.创建临时表
    # 2.sql语句查询
    # 3.删除临时表
    for index,i in enumerate(block_list(tmp_table,10000)):
        if index==0:
            write2db(i,tmp_table_name,mode='w',conn=conn)
        else:
            write2db(i,tmp_table_name,mode='w+',conn=conn)
    try:
        df_ = get_data_from_db(sql=sql,conn=conn)
    except Exception as e :
        print(e)
        df_ = pd.DataFrame()
    finally:
        # 创建metadata对象
        metadata = MetaData()
        # 定义要删除的表
        users_table = Table(tmp_table_name, metadata, autoload_with=conn)
        # 删除表
        with conn.connect() as conn:
            users_table.drop(conn)
    return df_

def update_table(sql, conn):
    """
    :param sql:
    :param conn:数据库连接
    :return:
    """
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
