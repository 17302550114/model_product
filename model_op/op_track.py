import os
import sys
import datetime
import pandas as pd
from dateutil import parser

""" 编写轨迹相关的通用算子
1.时间类：
（1）昼伏夜出：计算昼伏夜出/夜间出行情况  输入轨迹时间列表和昼/夜定义[6，22]；输出：出行天数/夜间出行天数
（2）夜间出行：计算夜间出行情况   输入轨迹时间列表和昼夜分界点列表[6，22]；
2.地址类：
（1）活动区域范围：基于地址经纬度数据计算大致活动范围  输入经纬度列表数据， 输出活动范围(圆？矩形？)

3.轨迹类：
（1）计算总里程：计算行驶总里程。 输入 经纬度数据和轨迹时间， 输出行驶总里程
（2） 
"""

def opTrackZFYC(list_track_sj:list=[],list_thred_time:list=[6,22])->tuple:
    """计算昼伏夜出
    Args:
        list_track_sj (list, optional): 轨迹时间列表. Defaults to [].
        list_thred_time (list, optional): 昼/夜定义时刻里列表，默认早6晚22. Defaults to [6,22].

    Returns:
        tuple: 结果元组（出行天数,昼伏夜出天数）
    """    
    if list_track_sj == []:
        return None,None
    list_sj = [parser.parse(i) for i in list_track_sj]
    gj_days = len(set([i.date() for i in list_sj]))
    zfyc_days = 0
    list_track_sj_detail = []
    for sj in list_sj:
        dict_sj = {}
        dict_sj["date"] = sj.date()
        dict_sj["hour"] = sj.hour
        list_track_sj_detail.append(dict_sj)
        df_sj_detail = pd.DataFrame(list_track_sj_detail)
    for date,group in df_sj_detail.groupby('date',as_index=False):
        list_hour = group['hour'].values
        flag = 0
        if len(list_hour) == 1: # 如果一天只有一个轨迹
            if not list_hour[0] in range(list_thred_time[0],list_thred_time[1]): # 轨迹时间在晚上
                zfyc_days += 1 # 昼伏夜出天数+1
        else:
            for i in list_hour:
                if i in range(list_thred_time[0],list_thred_time[1]):
                    flag = 1  # 白天有轨迹
            if flag: # 如果白天有轨迹，则不符合昼伏夜出
                continue
            zfyc_days += 1
    return gj_days,zfyc_days


def opTrackYJCX(list_track_sj:list=[],list_conditon:list=[6,22])->tuple:
    """计算夜间出行
    Args:
        list_track_sj (list, optional): 轨迹时间列表. Defaults to [].
        list_conditon (list, optional): 昼/夜定义时刻里列表，默认早6晚22. Defaults to [6,22].

    Returns:
        tuple: 结果元组（出行天数,夜间出行天数）
    """    
    if list_track_sj == []:
        return None,None
    list_track_sj = [parser.parse(i) for i in list_track_sj]

    track_yj  = [i.date() for i in list_track_sj if i.hour not in range(list_conditon[0],list_conditon[1])]
    yjcx_ts = len(set(track_yj))
    cxts = len(set([i.date() for i in list_track_sj]))
    return cxts,yjcx_ts

def opTrackJXCX(list_track_sj:list=[])->tuple:
    """计算间歇出现：近3天内有轨迹，且除去三天轨迹的其余轨迹，最小差值天数大于60天
    Args:
        list_track_sj (list, optional): 轨迹时间列表. Defaults to [].
    Returns:
        tuple: 结果元组（非近期轨迹天数,非近期轨迹与近期轨迹最小差值天数），返回结果中最小差值天数不为0的则为间歇出现
    """
    # 
    today = datetime.datetime.now()  # 当前日期时间
    近期轨迹 = [i for i in list_track_sj if abs((today - parser.parse(i))).days<3]
    if  近期轨迹 ==[]:
        return 0,0
    近期轨迹天数 = len(set([parser.parse(i).date() for i in 近期轨迹]))
    非近期轨迹 = [i for i in list_track_sj if i  not in 近期轨迹]
    if  非近期轨迹 ==[]:
        return 0,999
    非近期轨迹天数 = len(set([parser.parse(i).date() for i in 非近期轨迹]))
    # print(f"总轨迹数:{len(list_track_sj)}  ，近期轨迹数:{len(近期轨迹)}，非近期轨迹数:{len(非近期轨迹)}")
    近期轨迹_min_sj = min(近期轨迹)
    非近期轨迹_max_sj = max(非近期轨迹)
    days = (parser.parse(近期轨迹_min_sj) - parser.parse(非近期轨迹_max_sj)).days
    if days>60:
        return 非近期轨迹天数,days
    return 0,0