import json
import requests
import math
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
with open("INFO.json", "r") as json_read:
    info_contents = json.loads(json_read.read())

class public_data_gokr():
    def __init__(self, tablename, url_prefix, url_parameters, connection_string):
        self.tablename = tablename
        self.url = url_prefix+url_parameters
        self.connection_string=connection_string

    def crawler(self):
        print(self.url)
        res = requests.get(url=self.url).text
        jsondata = json.loads(res)
        totalcount = jsondata['body']['totalCount']
        maxpageno = math.ceil(totalcount/numOfRows)
        print(totalcount, maxpageno)
        engine = create_engine(self.connection_string)
        engine.execute(f"TRUNCATE TABLE {self.tablename};")
        total_df = pd.DataFrame()
        for pageno in range(1, maxpageno + 1):
            url = f"{self.url}&pageNo={pageno}"
            print(pageno, end=' ')
            res = requests.get(url=url, allow_redirects=False).text
            jsondata = json.loads(res)


            items = jsondata['body']['items']
            df = pd.DataFrame(items)
            df['REGT_ID'] = 'yejinjo'
            df['REG_DTTM'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # print(df)
            total_df = pd.concat([total_df, df])
            if ((pageno % 40) == 0):
                print()
        total_df.to_csv(f"{self.tablename}.csv", encoding='utf-8-sig')
        total_df.to_sql(name=f"{self.tablename}".lower(), con=engine, index=False, if_exists='append', chunksize=16000)
        print()

if __name__ == "__main__":
    numOfRows=100
    connection_string = f"snowflake://{info_contents['sf_user']}:{info_contents['sf_password']}@{info_contents['sf_host']}/{info_contents['sf_db']}/{info_contents['sf_schema']}?warehouse={info_contents['sf_wh']}&role={info_contents['sf_role']}"
    pdb = public_data_gokr(tablename="O_Mdcn_HtfsList",
                           url_prefix="http://apis.data.go.kr/1471000/HtfsInfoService2/getHtfsList?",
                           url_parameters=f"type=json&numOfRows={numOfRows}&serviceKey={info_contents['serviceKey']}",
                           connection_string=connection_string)
    pdb.crawler()