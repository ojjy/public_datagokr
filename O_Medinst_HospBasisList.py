from pd_utils import public_data_gokr, info
import pandas as pd
import requests
import json
import math
from datetime import datetime
import sqlalchemy

class O_Medinst_HospBasisList(public_data_gokr):
    def crawler(self):
        print(self.url)
        res = requests.get(url=self.url).text
        jsondata = json.loads(res)
        totalcount = jsondata['response']['body']['totalCount']
        maxpageno = math.ceil(totalcount / numOfRows)
        print(totalcount, maxpageno)
        engine = self.connect_db("postgres")
        engine.execute(f"TRUNCATE TABLE {self.tablename};")
        total_df = pd.DataFrame()
        # for pageno in range(1, maxpageno + 1):
        for pageno in range(1, 3):
            # for pageno in range(1, 3):
            url = f"{self.url}&pageNo={pageno}"
            print(pageno, end=' ')
            res = requests.get(url=url, allow_redirects=False).text
            jsondata = json.loads(res)
            items = jsondata['response']['body']['items']
            df = pd.DataFrame(items)
            df['REGT_ID'] = 'yjjo'
            df['REG_DTTM'] = datetime.now().strftime('%Y-%m-%d')
            print(df)
            total_df = pd.concat([total_df, df])
            if ((pageno % 40) == 0):
                print()
        total_df.columns = total_df.columns.str.lower()
        total_df.to_sql(name=f"{self.tablename}".lower(), index=False, con=engine, if_exists='replace', chunksize=16000)
                        # dtype={
                        #     'ykiho': sqlalchemy.types.VARCHAR(400),
                        #     'yadmNm': sqlalchemy.types.VARCHAR(150),
                        #     'clCd': sqlalchemy.types.VARCHAR(2),
                        #     'clCdNm': sqlalchemy.types.VARCHAR(150),
                        #     'sidoCd': sqlalchemy.types.VARCHAR(6),
                        #     'sidoCdNm': sqlalchemy.types.VARCHAR(150),
                        #     'sgguCd': sqlalchemy.types.VARCHAR(6),
                        #     'sgguCdNm': sqlalchemy.types.VARCHAR(150),
                        #     'emdongNm': sqlalchemy.types.VARCHAR(150),
                        #     'postNo': sqlalchemy.types.VARCHAR(6),
                        #     'addr': sqlalchemy.types.VARCHAR(500),
                        #     'telno': sqlalchemy.types.VARCHAR(30),
                        #     'hospUrl': sqlalchemy.types.VARCHAR(500),
                        #     'estbDd': sqlalchemy.types.VARCHAR(8),
                        #     'drTotCnt': sqlalchemy.types.INT,
                        #     'mdeptGdrCnt': sqlalchemy.types.INT,
                        #     'mdeptIntnCnt': sqlalchemy.types.INT,
                        #     'mdeptResdntCnt': sqlalchemy.types.INT,
                        #     'mdeptSdrCnt': sqlalchemy.types.INT,
                        #     'detyGdrCnt': sqlalchemy.types.INT,
                        #     'detyIntnCnt': sqlalchemy.types.INT,
                        #     'detyResdntCnt': sqlalchemy.types.INT,
                        #     'detySdrCnt': sqlalchemy.types.INT,
                        #     'cmdcGdrCnt': sqlalchemy.types.INT,
                        #     'cmdcIntnCnt': sqlalchemy.types.INT,
                        #     'cmdcResdntCnt': sqlalchemy.types.INT,
                        #     'cmdcSdrCnt': sqlalchemy.types.INT,
                        #     'XPos': sqlalchemy.types.VARCHAR(18),
                        #     'YPos': sqlalchemy.types.VARCHAR(18),
                        #     'distance': sqlalchemy.types.VARCHAR(10),
                        #     'REGT_ID': sqlalchemy.types.VARCHAR(10),
                        #     'REG_DTTM': sqlalchemy.types.DATE})
        print()

if __name__ == "__main__":
    numOfRows = 100
    pdb = O_Medinst_HospBasisList(tablename="O_Medinst_HospBasisList",
                           url_prefix="http://apis.data.go.kr/B551182/hospInfoService1/getHospBasisList1?",
                           url_parameters=f"_type=json&numOfRows={numOfRows}&serviceKey={info['serviceKey']}",
                           connection_string=None)
    pdb.crawler()
