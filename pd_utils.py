import json
import requests
import math
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import sqlalchemy

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
        engine = self.connect_db("postgres")
        engine.execute(f"TRUNCATE TABLE {self.tablename};")
        total_df = pd.DataFrame()
        for pageno in range(1, maxpageno + 1):
        # for pageno in range(1, 3):
            url = f"{self.url}&pageNo={pageno}"
            print(pageno, end=' ')
            res = requests.get(url=url, allow_redirects=False).text
            jsondata = json.loads(res)
            items = jsondata['body']['items']
            df = pd.DataFrame(items)
            df['REGT_ID'] = 'yjjo'
            df['REG_DTTM'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # print(df)
            total_df = pd.concat([total_df, df])
            if ((pageno % 40) == 0):
                print()
        total_df.columns = total_df.columns.str.lower()
        total_df.to_sql(name=f"{self.tablename}".lower(), index=False, con=engine, if_exists='append', chunksize=16000, dtype={
            'ENTRPS':sqlalchemy.types.VARCHAR(128),
            'PRDUCT':sqlalchemy.types.VARCHAR(1024),
            'STTEMNT_NO':sqlalchemy.types.VARCHAR(1024),
            'REGIST_DT':sqlalchemy.types.VARCHAR(1024),
            'REGT_ID':sqlalchemy.types.VARCHAR(10),
            'REG_DTTM':sqlalchemy.types.DATE})
        print()

    def connect_db(self, dbms_type):
        if dbms_type == "snowflake":
            # 'snowflake://<user_login_name>:<password>@<account_identifier>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>'
            self.connection_string = f'snowflake://{info_contents["sf_user"]}:{info_contents["sf_pwd"]}@' \
                                     f'{info_contents["sf_host"]}/{info_contents["sf_db"]}/{info_contents["sf_schema"]}?' \
                                     f'warehouse={info_contents["sf_wh"]}&role={info_contents["sf_role"]}'
        elif dbms_type == "mysql":
            self.connection_string = f'mysql+pymysql://{info_contents["mysql_user"]}:{info_contents["mysql_pwd"]}@' \
                                     f'{info_contents["mysql_host"]}:{info_contents["mysql_port"]}/{info_contents["mysql_db"]}'
            print(self.connection_string)
        elif dbms_type == "postgres":
            self.connection_string = f'postgresql+psycopg2://{info_contents["postgres_user"]}:{info_contents["postgres_pwd"]}@' \
                                     f'{info_contents["postgres_host"]}:{info_contents["postgres_port"]}/{info_contents["postgres_db"]}'
        elif dbms_type == "mssql":
            self.connection_string = f'mssql+pymssql://{info_contents["mssql_user"]}:{info_contents["mssql_pwd"]}@' \
                                     f'{info_contents["mssql_host"]}:{info_contents["mssql_port"]}/{info_contents["mssql_db"]}'
        else:
            raise "UNSUPPORTED DBMS"

        return create_engine(self.connection_string)


if __name__ == "__main__":
    numOfRows=100
    pdb = public_data_gokr(tablename="O_Mdcn_HtfsList",
                           url_prefix="http://apis.data.go.kr/1471000/HtfsInfoService2/getHtfsList?",
                           url_parameters=f"type=json&numOfRows={numOfRows}&serviceKey={info_contents['serviceKey']}",
                           connection_string=None)
    pdb.crawler()