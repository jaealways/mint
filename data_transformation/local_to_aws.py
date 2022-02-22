import pandas as pd
import pymysql

from db_env import DbEnv
from mongo_to_sql import MongoToSQL

class LocalToAWS:
    def connect_sql(self):
        conn = pymysql.connect(host='kuggle.csr6lxslx7sg.ap-northeast-2.rds.amazonaws.com', user='kuggle',
                               password='musiccow', db='mu_tech', charset='utf8')
        cursor = conn.cursor()
        conn.commit()

        return conn, cursor

    def db_aws(self, conn, cursor, sql):
        cursor.execute(sql)
        conn.commit()

        return conn, cursor

    def update_df_to_aws(self, df):
        for idx, x in df.iterrows():
            for i, v in x.items():
                cursor.execute(sql.format(idx, str(i), v))
                conn.commit()



    # def update_sql_to_aws(self, col_local, col_aws):



# mongo_sql = MongoToSQL()

df_per = pd.read_pickle("../storage/df_raw_data/df_per_month_12.pkl")
df_beta = pd.read_pickle("../storage/df_raw_data/df_beta.pkl")

df_per_date = df_per.loc[48, :]
df_beta_date = df_beta.loc[48, :]

conn_sql, cursor_sql = LocalToAWS().connect_sql()

sql = f"""SELECT * FROM daily_beta """
cursor_sql.execute(sql)
data_sql = cursor_sql.fetchall()


sql = f"""CREATE TABLE IF NOT EXISTS daily_beta 
(num int(11) NOT NULL, date varchar(255) NOT NULL, value float(15) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
conn, cursor = LocalToAWS().db_aws(conn_sql, cursor_sql, sql)

sql = f"""CREATE TABLE IF NOT EXISTS daily_per 
(num int(11) NOT NULL, date varchar(255) NOT NULL, value float(15)) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
conn, cursor = LocalToAWS().db_aws(conn_sql, cursor_sql, sql)

# sql = f"""INSERT INTO daily_beta(num, date, value) VALUE(%d, %s, %f);"""
# LocalToAWS().update_df_to_aws(df_beta)

sql = """INSERT INTO daily_per(num, date, value) VALUES({0}, {1}, {2});"""
LocalToAWS().update_df_to_aws(df_per)
