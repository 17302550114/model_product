import os
import re
import sys
import math
import json
import time
import base64
import socket
import logging
import psycopg2
import datetime
import requests

from PIL import Image
from io import BytesIO
import warnings
warnings.filterwarnings('ignore')


socket.setdefaulttimeout(1)
import pandas as pd 
from sqlalchemy import types, create_engine
import warnings
warnings.simplefilter("ignore")
import numpy as np
from tqdm import tqdm
tqdm.pandas(desc='pandas bar')

from math import radians,fabs,cos,asin,sin,sqrt
from dateutil import parser
import datetime
from collections import namedtuple
import sqlite3
import difflib
from base64 import b64decode
from tools.admin_div_of_china.adoc import *

with open('/home/zhxz_model/setting.json') as j:
    settings = json.load(j)
    
# 工程基础路径
PROJECT_BASE_PATH = settings['sys_setting']["project_base_path"]
sys.path.append(PROJECT_BASE_PATH) # 添加路径




def init_log(logname, filename, level=logging.INFO, console=True):
    # make log file directory when not exists
    directory = os.path.dirname(filename)
    if not os.path.exists(directory):
            os.makedirs(directory)
    logging.basicConfig(level = level,
                        format="%(asctime)s %(name)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s",
                        filename=filename,
                        filemode='a')
    logger = logging.getLogger(logname)
    if console:
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        logger.addHandler(console)
    return logger


def my_logger(file_path,log_name):
    """
    日志输出
    :param log_name: 日志输出的文件名
    :return: None
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    log_name = file_path + log_name
    # print("日志路径",log_name)
    if not os.path.exists(file_path):
        os.mkdir(file_path)
        try:
            # 尝试使用mknod，如果在UNIX-like系统上可用
            os.mknod(log_name)
        except AttributeError:
            # 如果在Windows上，使用open来创建一个空文件
            with open(log_name, 'x') as file:
                pass
    else:
        pass
    fh = logging.FileHandler(log_name, mode='a',encoding='utf-8')
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

def update_status(status, taskid, number=0):
    conn_pg = psycopg2.connect(database='zhxz_mx', user='zhxz_mx', password='zhxz_mx', host='50.32.24.61', port='5432')
    """
    更新任务状态
    :param status: 任务状态，0：待分析，1：分析中，2：分析结束，3：分析失败
    :param taskid:任务ID
    :return:
    """
    if taskid == 'timingtask':
        pass
    else:
        if status==2:
            t = datetime.datetime.now()
            sql = f"update t_model_task set task_status={status},end_time='{t}',number='{number}' where id={taskid}"
        else:
            sql = f"update t_model_task set task_status={status} where id={taskid}"
        
        cur = conn_pg.cursor()
        try:
            cur.execute(sql)
        except Exception as e:
            log_info = f"pg数据库提交失败：{e},sql:{sql}"
            print(log_info)
            logging.info(log_info)
        else:
            conn_pg.commit()
        finally:
            pass
    
    
def block_list(list_data,n):
    for i in range(0,len(list_data),n):
        yield list_data[i:i+n]
        
def hot2list(x):
    sx_code={"1":"寄住人口","2":"房屋出租户房主","3":"待定","4":"待定","5":"社区治保人员","6":"养犬人员","7":"有机动车人员","8":"待定","9":"精神病和呆傻人员","10":"不准出境人员","11":"待定","12":"境外常住人员","13":"境外临时住宿人员","14":"受害人","15":"证人","16":"空挂户","17":"流出人员","18":"从业人员","19":"旅馆住宿人员","20":"纠纷当事人","21":"网吧上网人员","22":"不安定因素人员","23":"灾害事故伤亡人员","24":"走失人员","25":"客运从业人员","26":"典当人员","27":"租赁汽车人员","28":"托管户","29":"待定","30":"被盘查人员","31":"工作对象","32":"重点人口","33":"在押人员","34":"涉警人员","35":"涉案嫌疑人","36":"违法行为人","37":"吸毒人员","38":"审查嫌疑人","39":"社区矫正对象","40":"帮教对象","41":"两劳人员","42":"涉稳人员","43":"涉事人员","44":"待定","45":"重点人员","46":"待定","47":"消防违法人","48":"待定","49":"待定","50":"待定","51":"待定","52":"待定","53":"待定","54":"待定","55":"待定","56":"待定","57":"待定","58":"待定","59":"待定","60":"历史暂住人口"}
    hot=x
    sx_list=[i.start() for i in re.finditer('1',hot)]
    sx_zw=[]
    for k in sx_list:
        inde=str(k+1)
        sx=sx_code[inde]
        sx_zw.append(sx)
    return ','.join(sx_zw)

def isll(x):
    '''
    判断手机号后4位是否相同
    '''
    if pd.isna(x):
        return 0
    elif len(set(list(x[-4:])))== 1:
        return 1
    else:
        return 0


def check_sfz(x):
    # 匹配身份证号
    pattern = '^([1-9]\d{5}[12]\d{3}(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])\d{3}[0-9xX])$'
    if x is np.nan or x is None:
        return False
    else:
        return bool(re.findall(pattern, x))

def get_h_m_s(seconds):
    if seconds ==0:
        return "0秒"
    m,s = divmod(int(seconds),60)
    h,m = divmod(m,60)
    str_ = f"{h}小时{m}分钟{s}秒"
    if h==0:
        str_ = re.sub("\d+小时",'',str_)
    if m==0:
        str_ = re.sub("\d+分钟",'',str_)
    if s==0:
        str_ = re.sub("\d+秒",'',str_)
    return str_    

def prosess_whcd(x):
    list_whcd = ['中等', '未知', '初中', '大学专科', '大学本科', '小学', '技工', '研究生','高中']
    for i in list_whcd:
        if re.findall(i,str(x))!=[]:
            return i
        else:
            pass
    return '未知'


def format_sj(x):
    try:
        x = str(x)
        sj = parser.parse(x)
    except Exception as e:
        try:
            sj = parser.parse(time.strftime('%Y%m%d%H%M%S',time.localtime(int(x))))
        except Exception as e:
            sj = ''
    return str(sj)[0:19]

def hav(theta):
    s = sin(theta/2)
    return s*s

def get_distance_by_lat_lon(lat0,lon0,lat1,lon1):
    EARTH_RADIUS = 6371
    lat0 = radians(lat0)
    lon0 = radians(lon0)
    lat1 = radians(lat1)
    lon1 = radians(lon1)
    dlng = fabs(lon0-lon1)
    dlat = fabs(lat0-lat1)
    h = hav(dlat) + cos(lat0)*cos(lat1)*hav(dlng)
    distance = 2*EARTH_RADIUS*asin(sqrt(h))
    return round(distance*1000,1)

class Adoc(object):
    '''
    area 表结构:
    
    '''
    def __init__(self,):
        path = '/home/zhxz/zhxzModel/tools/data.sqlite'
        conn = sqlite3.connect(path)
        self.cur = conn.cursor()
        
    def get_area_info(self, areaid: str)->namedtuple:
        '''
        Args:
            areaid: str areaid like '360102'
        
        Returns:
            admin_div: ('code', 'province', 'city', 'area') like  Admin_div(code='360102', province='江西省', city='南昌市', area='东湖区')
        '''
        sql = f'''
        SELECT
            area1.code,
            province.name AS provincename,
            city.name AS cityname,
            area1.name AS areaname            
        FROM
            ( SELECT * FROM area WHERE area.code = '{areaid}' ) area1
            LEFT JOIN city ON area1.cityCode = city.code
            LEFT JOIN province ON area1.provinceCode = province.code
        '''
        if not areaid or len(areaid) != 6:
            return None
        Admin_div = namedtuple('Admin_div', ['code', 'province', 'city', 'area'])
        self.cur.execute(sql)
        _item = self.cur.fetchone()
        if _item:
            item = Admin_div._make(_item)
            return item
        else:
            return None

def get_rxgj(sfz,sjsx,sjxx):
    datas = {
        "senderId":"C32-01000003",
        "serviceId":"Y32-01001393",
        "xm":"张大",
        "dwbm":"320100001",
        "zjhm":"231005198401240518",
        "dwmc":'情报大队',
        "type":"2",
        "data":{
            "entity_id":{
                "id":f"{sfz}",
                "entity_type":"PERSON_IDENTIFIED"
            },
            "page_size":1000,
            "marker":"",
            "period":{
                "start":sjsx,
                "end":sjxx
                },
                "reversed":True
            },
        "param":f'''http://50.32.23.122:30080/engine/entity-service/v1/entities/PERSON_IDENTIFIED/{sfz}/tracks''',
    }
    url = f'''http://50.32.14.25:8099/sso/getData'''
    header = {
            "Content-Type": "application/json",
            "Connection":"close"
        }
    response = requests.post(url,data=json.dumps(datas),headers = header,timeout=30)
    js = json.loads(response.text)
    result = json.loads(js["data"])
    return result

def utc2time_(x):
    time_ = parser.parse(x) + datetime.timedelta(hours=+8)
    time = datetime.datetime.strftime(time_,"%Y%m%d%H%M%S")
    return str(parser.parse(time))

def time2utc_(x):
    time_ = parser.parse(x) + datetime.timedelta(hours=-8)
    time = datetime.datetime.strftime(time_,"%Y-%m-%dT%H:%M:%SZ")
    return time

def deal(sfz,sjsx,sjxx):
    sjsx = str(time2utc_(sjsx))
    sjxx = str(time2utc_(sjxx))
    try:
        js = get_rxgj(sfz,sjsx,sjxx)
        num = len(js['tracks'])
        if num == 0:
            return pd.DataFrame(columns=["SFZ","RECORD_COUNT","DEVICE_ID","REGION_ID","CAMERA_IDX","FACE_SCORE","URL","URL_XT","SJ"])
        list_ = []
        for item in js['tracks']:
            dict_ = {}
            dict_['SFZ'] = sfz
            dict_["RECORD_COUNT"]  = num
            dict_["DEVICE_ID"] =''
            dict_["REGION_ID"]= item["camera_id"]["region_id"]
            dict_["CAMERA_IDX"]= item["camera_id"]["camera_idx"]
            dict_["FACE_SCORE"] = item["object"]["face"]["face_score"]
            dict_['URL'] = item['portrait_image']['url']
            dict_['URL_XT'] = item['panoramic_image']['url'] # 
            dict_['SJ'] = utc2time_(item['captured_time'])
            list_.append(dict_)
        return pd.DataFrame(list_)
    except Exception as e :
        print(f"人像接口获取失败{e}")
        return pd.DataFrame(columns=["SFZ","RECORD_COUNT","DEVICE_ID","REGION_ID","CAMERA_IDX","FACE_SCORE","URL","URL_XT","SJ"])


        
def get_std_addr_(x,dz:str,req_list:list):
    '''
    地址标准化
    :param x: pd.Series
    :param dz: 地址对应字段名
    :param req_list:需要的标准化数据，['xcoord', 'ycoord', 'zylx', 'zyid', 'std_addr', 'pl_std_zyid','pl_std_addr',
    'pl_std_hs_zyid', 'pl_std_hs_addr', 'sfid', 'xzqh', 'jd','sq', 'xzqmc', 'jdmc', 'sqmc', 'fjmc',
    'pcsmc', 'pcsbm', 'zrqbm','zrqmc']
    :return:
    '''
    def get_std_address(dz):
        '''
        根据地址获取标准化地址
        :param dz: 输入地址
        :return:返回标准化地址
        '''
        try:
            response = requests.get(url=f'http://pgis.nkg.js:8081/geost?address={dz}&city=320100',timeout=3)
            result = json.loads(response.text)["result"]
            std_addr = result["standardAddress"]
            return std_addr
        except:
            return ''
    try:
        dz = x[dz]
        std_dz = get_std_address(dz)
        if std_dz=='':
            std_dz = dz
        response = requests.get(url=f'http://10.33.72.170:8081/poi?address={std_dz}&city=320100',timeout=3)
    except Exception as e:
        print("地址标准化接口错误:",e)
        return ['']*len(req_list)
    try:
        result = json.loads(response.text)
        std_result = pd.DataFrame([result["result"]])
        std_result = std_result[req_list]
        value = std_result.values[0]
        return list(value)
    except Exception as e:
        print(f"未查询到归一化地址:{e},查询地址：{dz}")        

def print_info(info,prefix=''):
    try:
        info = f"prefix_{str(prefix)}:{info}" 
        logging.info(info)
        print(info)
    except Exception as e:
        print(f"日志输出错误:{e}")
    
def judgeAge(sfzh,time = 'now'):
    try:
        sfzh = str(sfzh)
        if len(sfzh)==18:
            year = str(sfzh)[6:10]
            month_day = sfzh[10:14]
        elif len(sfzh)==8:
            year = str(sfzh)[0:4]
            month_day = sfzh[4:8]
        else:
            return 999
        now = datetime.datetime.now()
        if time !='now':
            now = parser.parse(time)
        now_year = now.year
        now_month = str(now.month)
        now_day = str(now.day)
        
        if len(now_month)==1:
            now_month = '0'+ now_month
        if len(now_day)==1:
            now_day = "0" + str(now_day)
        now_date = now_month + now_day
        age = now_year - int(year)
        if now_date<month_day:
            age = age-1
        return int(age)
    except Exception as e :
        print(e)
        return 999
    
def get_area_div(x):
    '''
        输入身份证前6位，输出[省,市,县]列表
    '''
    adminDiv = adminDivOfChina()
    with adminDiv.query() as query:
        result = query(x)
        return [result.province,result.city,result.area]

def b642Image(b64):
    try:
        return Image.open(BytesIO(base64.b64decode(b64)))
    except Exception as e:
        return ''
        
if __name__ == '__main__':
    admin_div = Adoc()
    print(admin_div.get_area_info('360102'))