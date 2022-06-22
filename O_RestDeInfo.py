import json
import os.path

import requests
import math
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import sqlalchemy
from bs4 import BeautifulSoup
import numpy as np
from pdutils import public_data_gokr

with open("info.json", "r") as json_read:
    info = json.loads(json_read.read())

class RestDeInfo(public_data_gokr):
    def crawler(self):
        def month_year_iter(start_month, start_year, end_month, end_year):
            ym_start = 12 * start_year + start_month - 1
            ym_end = 12 * end_year + end_month - 1
            for ym in range(ym_start, ym_end):
                y, m = divmod(ym, 12)
                yield y, m + 1

        engine = self.connect_db("snowflake")
        total_df = pd.DataFrame()
        info_arr=[]
        for year, month in month_year_iter(1, 2017, 6, 2022):
            if month<10:
                month = "0"+str(month)
            url = f"{self.url}&solYear={year}&solMonth={month}"
            print(year, month, url)
            res = requests.get(url=url)
            soup = BeautifulSoup(res.content, 'html.parser')
            items = soup.find_all('item')
            print(items)
            for idx, item in enumerate(items):
                info = {}
                info['dateKind'.lower()] = item.find('dateKind'.lower()) if item.find('dateKind'.lower()) == None else item.find('dateKind'.lower()).get_text()
                info['dateName'.lower()] = item.find('dateName'.lower()) if item.find('dateName'.lower()) == None else item.find('dateName'.lower()).get_text()
                info['isHoliday'.lower()] = item.find('isHoliday'.lower()) if item.find('isHoliday'.lower()) == None else item.find('isHoliday'.lower()).get_text()
                info['locdate'] = item.find('locdate') if item.find('locdate') == None else item.find('locdate').get_text()
                info['seq'] = item.find('seq') if item.find('seq') == None else item.find('seq').get_text()
                info_arr.append(info)
        df = pd.DataFrame(info_arr)
        df = df.replace(np.nan, value="")
        df.rename(columns={'datekind':'HOL_KND_CD'}, inplace=True)
        df.rename(columns={'datename':'HOL_NM'}, inplace=True)
        df.rename(columns={'isholiday':'GOF_HOL_YN'}, inplace=True)
        df.rename(columns={'locdate':'BASE_DD'}, inplace=True)
        df.rename(columns={'seq':'BASE_DD_SEQ'}, inplace=True)
        df['LOAD_DTTM'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        engine.execute(f"TRUNCATE TABLE {self.tablename}")
        df.to_sql('WSTC_HOL', con=engine, if_exists="append", index=False)

if __name__ == "__main__":
    numOfRows=100
    pdb = RestDeInfo(tablename="RestDeInfo",
                     url_prefix="http://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getRestDeInfo?",
                     url_parameters=f"numOfRows={numOfRows}&serviceKey={info['serviceKey']}",
                     connection_string=None)
    pdb.crawler()
