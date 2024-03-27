from contextlib import contextmanager
from collections import namedtuple
import sqlite3
import os
import sys
sys.path.append('/home/zhxz_model')

class adminDivOfChina(object):
    '''
    area 表结构:
    '''
    def __init__(self,):
#         self.path = 'admin_div_of_china/admin_div.sqlite'
        self.path = '/home/zhxz_model/tools/admin_div_of_china/admin_div.sqlite'
    @contextmanager
    def query(self,):
        conn = sqlite3.connect(self.path)
        self.cur = conn.cursor()
        try:
            yield self.__get_area_info
        finally:
            conn.close()
        
    def __get_area_info(self, areaid: str)->namedtuple:
        '''
        Args:
            areaid: str areaid like '360102'
        
        Returns:
            admin_div: ('code', 'province', 'city', 'area') like  admin_div(code='360102', province='江西省', city='南昌市', area='东湖区')
        '''
        sql = f'''
        SELECT
            area1.code,
            province.div AS provincename,
            city.div AS cityname,
            area1.div AS areaname            
        FROM
            ( SELECT * FROM country WHERE country.code = '{areaid}' ) area1
            LEFT JOIN city ON area1.citycode = city.code
            LEFT JOIN province ON area1.provincecode = province.code
        '''
        Admin_div = namedtuple('admin_div', ['code', 'province', 'city', 'area'])
        self.cur.execute(sql)
        _item = self.cur.fetchone()
        
        if not _item:
            _item = [areaid, None, None, None]
        item = Admin_div._make(_item)
        return item


    
# admin_div = adminDivOfChina()
# admin_div.get_area_info('420204')
    
if __name__ == '__main__':
    adminDiv = adminDivOfChina()
    with adminDiv.query() as query:
        print(query('420204'))
        print(query('360102')) 