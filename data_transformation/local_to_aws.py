import pandas as pd
import pymysql
import json


class LocalToAWS:
    def connect_sql(self):
        try:
            with open("../storage/key.json", "r") as env:
                env_dict = json.load(env)
        except:
            with open("./storage/key.json", "r") as env:
                env_dict = json.load(env)
        aws_db_pw = env_dict["aws_password"]
        conn = pymysql.connect(host='kuggle.csr6lxslx7sg.ap-northeast-2.rds.amazonaws.com', user='kuggle',
                               password=aws_db_pw, db='mu_tech', charset='utf8')
        cursor = conn.cursor()
        conn.commit()

        return conn, cursor

    def update_local_to_aws(self, conn_sql, cursor_sql, conn_aws, cursor_aws, table):
        sql = f"SELECT * from {table}"
        cursor_sql.execute(sql)
        tuple_local = cursor_sql.fetchall()

        cursor_aws.execute(sql)
        tuple_aws = cursor_aws.fetchall()

        diff_local = tuple(set(tuple_local) - set(tuple_aws))
        value_sql = ('%s, ' * len(tuple_local[0]))[:-2]

        sql = f"INSERT INTO {table} VALUES ({value_sql})"
        cursor_aws.executemany(sql, diff_local)
        conn_aws.commit()
