from django.shortcuts import render
from faker import Faker
import sys
import os
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
# Create your views here.

def userList(request):
    print("111")
    # dataList=userInfo.objects.all()
    dataList = get_data_from_db(sql='select * from theme_yryd',conn=conn_mysql)
    print(dataList)
    
    return render(request,"userList.html" ,{"dataList":dataList})