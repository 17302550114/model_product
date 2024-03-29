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
sys.path.append('/home/zhxz_model')
os.chdir(os.path.dirname(os.path.abspath(__file__)))
from sql_conn import *
from multiprocessing import *
from tools.other_tool import *

def error(error):
    print(error)

faker = Faker(["zh_CN"])
# faker.locale = 'zh_CN'
faker.city_suffix = '南京'

conn_mysql = db_conn_init("db_mysql_local")
now = datetime.datetime.now()
file_path = os.path.dirname(__file__) + '/logs/'
my_logger(file_path,"更新模拟数据.log")


def update_ry_jbxx(num=10):
    list_ = []
    list_rylb = ["常住人口","常住人口","常住人口","常住人口","常住人口",'暂住人口','暂住人口','流动人口']
    for i in range(num):
        dict_ = {}
        dict_["name"] = faker.name()
        dict_["userid"] = faker.ssn()
        dict_["phone"] = faker.phone_number()
        dict_["fwcs"] = faker.company()
        dict_["zw"] = faker.job()
        dict_["plateno"] = faker.license_plate()
        dict_['rylb'] = random.choice(list_rylb)
        dict_['xzzxz'] = faker.address()
        list_.append(dict_)
    data_ry_jbxx = pd.DataFrame(list_)
    data_ry_jbxx["rksj"] = str(datetime.datetime.now())[0:19]
    write2db(data_ry_jbxx,'ry_jbxx',mode='w+',conn=conn_mysql)
    print_info(f"新增{data_ry_jbxx.shape[0]}条人员基本信息数据")
    return data_ry_jbxx

def update_aj_jbxx(num=50):
    list_ = []
    now = datetime.datetime.now()
    st_time = now + datetime.timedelta(days=-2)
    for i in range(num):
        dict_ = {}
        dict_["ajbh"] = "AJ" + ''.join([str(faker.random_digit())  for i in range(20)])
        dict_["ajmc"] = random.choice([faker.name(),faker.company()]) + "被"  + random.choice(["盗"]) + "案"
        dict_["ajlb"] = random.choice(["盗窃案"])
        slsj = str(faker.date_time_between(now + datetime.timedelta(days=-1),now))[0:19]
        fasjsx = faker.date_time_between(now + datetime.timedelta(days=-2),now + datetime.timedelta(days=-1))
        fasjxx = faker.date_time_between(now + datetime.timedelta(days=-1),now)
        if str(slsj)<str(fasjxx):
            continue
        dict_["slsj"] = slsj
        dict_["fasjsx"] = fasjsx
        dict_["fasjxx"] = fasjxx
        dict_["jyaq"] = faker.text()[0:1000]
        dict_['jd'] = random.uniform(118,120)
        dict_['wd'] = random.uniform(31,33)
        list_.append(dict_)
    data_aj_jbxx = pd.DataFrame(list_)
    data_aj_jbxx['fasjsx'] = data_aj_jbxx['fasjsx'].apply(format_sj)
    data_aj_jbxx['fasjxx'] = data_aj_jbxx['fasjxx'].apply(format_sj)
    data_aj_jbxx["rksj"] = str(datetime.datetime.now())[0:19]
    write2db(data_aj_jbxx,'aj_jbxx',mode='w+',conn=conn_mysql)
    print_info(f"新增{data_aj_jbxx.shape[0]}条案件基本信息数据")
    return data_aj_jbxx

def update_xyr_jbxx(num=10):
    data_aj = get_data_from_db(sql="select ajbh,slsj from aj_jbxx",conn=conn_mysql)
    list_ajbh = list(set(data_aj["ajbh"]))

    data_ry = get_data_from_db(sql="select userid from ry_jbxx",conn=conn_mysql)
    list_ry = list(set(data_ry["userid"]))
    
    list_ = []
    for i in range(num):
        list_ry.append(faker.ssn())
        dict_ = {}
        dict_["ajbh"] = random.choice(list_ajbh)
        dict_["userid"] = random.choice(list_ry)
        dict_["zhdw"] = faker.company()
        list_.append(dict_)
    data_xyr_jbxx = pd.DataFrame(list_)
    data_xyr_jbxx = data_xyr_jbxx.merge(data_aj)
    data_xyr_jbxx["zhrq"] = data_xyr_jbxx["slsj"].apply(lambda x: parser.parse(x) +  datetime.timedelta(days=random.choice(range(10))))
    data_xyr_jbxx = data_xyr_jbxx[data_xyr_jbxx["zhrq"]<str(now)]
    data_xyr_jbxx["zhrq"] = data_xyr_jbxx["zhrq"].apply(lambda x: str(x)[0:10])
    data_xyr_jbxx = data_xyr_jbxx.drop(columns=['slsj'])
    data_xyr_jbxx["rksj"] = str(datetime.datetime.now())[0:19]
    write2db(data_xyr_jbxx,'aj_xyrxx',mode='w+',conn=conn_mysql)
    print_info(f"新增{data_xyr_jbxx.shape[0]}条案件嫌疑人数据")
    return data_xyr_jbxx
    
def update_rx_gj(num=1000):
    data_device = get_data_from_db(sql="select device_id from device_jbxx where device_type='卡口设备'",conn=conn_mysql)
    list_device = list(set(data_device["device_id"]))
    data_ry = get_data_from_db(sql="select userid from ry_jbxx",conn=conn_mysql)
    list_ry = list(set(data_ry["userid"]))
    list_other_ssn = [faker.ssn()  for i in range(num//100)]
    list_ry = list_ry + list_other_ssn
    list_ = []
    for i in range(num):
        dict_ = {}
        dict_["userid"] = random.choice(list_ry)
        dict_["device_id"] = random.choice(list_device)
        dict_["passtime"] = faker.date_time_between(now + datetime.timedelta(hours=-2),now)
        list_.append(dict_)
    data_gj_rx = pd.DataFrame(list_)
    data_gj_rx['passtime'] = data_gj_rx['passtime'].apply(format_sj)
    data_gj_rx["rksj"] = str(datetime.datetime.now())[0:19]
    write2db(data_gj_rx,'gj_rxgj',mode='w+',conn=conn_mysql)
    print_info(f"新增{data_gj_rx.shape[0]}条人像轨迹数据")
    
def update_cl_gj(num=100):
    data_device = get_data_from_db(sql="select device_id from device_jbxx where device_type='卡口设备'",conn=conn_mysql)
    list_device = list(set(data_device["device_id"]))
    data_ry = get_data_from_db(sql="select userid,plateno from ry_jbxx",conn=conn_mysql)
    list_ry = list(set(data_ry["userid"]))
    dict_plateno = dict(zip(data_ry["userid"],data_ry['plateno']))
    list_ = []
    for i in range(num):
        dict_ = {}
        list_ry.append(faker.ssn())
        userid = random.choice(list_ry)
        dict_["userid"] = userid
        dict_["device_id"] = random.choice(list_device)
        dict_["passtime"] = faker.date_time_between(parser.parse("2024-01-01"),parser.parse('2024-03-20'))
        list_.append(dict_)
    data_gj_clxx = pd.DataFrame(list_)
    data_gj_clxx['passtime'] = data_gj_clxx['passtime'].apply(format_sj)
    data_gj_clxx["plateno"] = data_gj_clxx["userid"].map(dict_plateno)
    data_gj_clxx["plateno"] = data_gj_clxx["plateno"].apply(lambda x:faker.license_plate() if pd.isna(x) else x)
    data_gj_clxx["rksj"] = str(datetime.datetime.now())[0:19]
    write2db(data_gj_clxx,'gj_clgj',mode='w+',conn=conn_mysql)
    print_info(f"新增{data_gj_clxx.shape[0]}条车辆轨迹数据")
    


def update_gj_lgzs(num=100):
    data_device = get_data_from_db(sql="select device_id from device_jbxx where device_type='旅馆'",conn=conn_mysql)
    list_device = list(set(data_device["device_id"]))
    data_ry = get_data_from_db(sql="select userid from ry_jbxx",conn=conn_mysql)
    list_ry = list(set(data_ry["userid"]))
    list_hour = [random.randint(8,18)] + [random.randint(18,23)]*10 + [random.randint(0,6)]*5
    list_hour = ['0' + str(i) for i in list_hour if i<10] + [str(i) for i in list_hour if i>=10]
    def gen_guestogeid():
        str_togeid = random.choice(list_device) + re.sub('-| |:','',str(faker.date_time_between(now+datetime.timedelta(days=-1),now))[0:19])
        str_togeid = str_togeid + ''.join([str(faker.random_digit()) for i in range(4)])
        return str_togeid
    list_ = []
    list_guestogeid = [gen_guestogeid() for i in range(num//2)] 
    for i in range(num):
        dict_ = {}
        list_ry.append(faker.ssn())
        dict_["userid"] = random.choice(list_ry)

        guestoge = random.choice(list_guestogeid)
        dict_["guestogeid"] = guestoge
        dict_["corpid"] = guestoge[0:15]
        checkintime = guestoge[15:29]
        checkintime = str(parser.parse(checkintime))
        checkintime = re.sub(r" (\d{1,2}):", f" {random.choice(list_hour)}:", checkintime)
        dict_["checkintime"] = checkintime
        checkouttime = parser.parse(str(checkintime)) + datetime.timedelta(hours=random.randint(12,23))
        if checkouttime>now:
            continue
        dict_["checkouttime"] = checkouttime
        dict_["roomnum"] = guestoge[-4:]
        list_.append(dict_)
    data_ry_jbxx = pd.DataFrame(list_)
    data_ry_jbxx['checkintime'] = data_ry_jbxx['checkintime'].apply(format_sj)
    data_ry_jbxx['checkouttime'] = data_ry_jbxx['checkouttime'].apply(format_sj)
    # 把入住人数大于4个的去掉
    data_rs = data_ry_jbxx.groupby(by=["guestogeid"])["userid"].agg(lambda x: len(set(x))).to_frame().reset_index()
    count_rs = data_rs[data_rs["userid"]<4]
    data_gj_lgzs = data_ry_jbxx[data_ry_jbxx["guestogeid"].apply(lambda x: x in list(set(count_rs["guestogeid"])))]
    data_gj_lgzs["rksj"] = str(datetime.datetime.now())[0:19]
    write2db(data_gj_lgzs,'gj_lgzs',mode='w+',conn=conn_mysql)
    print_info(f"新增{data_gj_lgzs.shape[0]}条旅馆住宿数据")

if __name__ == '__main__':
    print_info("*"*100)
    pool = Pool(10)
    result_ry_jbxx = pool.apply_async(func=update_ry_jbxx,args=(), error_callback=error)
    result_aj_jbxx = pool.apply_async(func=update_aj_jbxx,args=(), error_callback=error)
    result_xyr_jbxx = pool.apply_async(func=update_xyr_jbxx,args=(), error_callback=error)
    result_gj_rxgj = pool.apply_async(func=update_rx_gj,args=(), error_callback=error)
    result_gj_clgj = pool.apply_async(func=update_cl_gj,args=(), error_callback=error)
    result_gj_lgzs = pool.apply_async(func=update_gj_lgzs,args=(), error_callback=error)
    pool.close()
    pool.join()
    print_info("数据更新完毕")