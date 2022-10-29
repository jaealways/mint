from pymongo import MongoClient
import pymysql
import json

# mongodb 저장 형식을 sql 형식으로 옮기기, 연산처리 빠르게

class DbEnv:
    def connect_mongo(self, db, col):
        client = MongoClient('localhost', 27017)
        conn = client['%s' % db]['%s' % col]

        return conn

    def connect_sql(self):
        try:
            with open("./storage/key.json", "r") as env:
                env_dict = json.load(env)
        except:
            with open("../storage/key.json", "r") as env:
                env_dict = json.load(env)
        sql_db_pw = env_dict["sql_password"]
        conn = pymysql.connect(host='127.0.0.1', user='root', password=sql_db_pw, db='mu_tech', charset='utf8mb4')
        # conn = pymysql.connect(host='kuggle.csr6lxslx7sg.ap-northeast-2.rds.amazonaws.com', user='kuggle', password='musiccow', db='mu_tech', charset='utf8mb4')

        cursor = conn.cursor()
        conn.commit()

        return conn, cursor

    def create_db(self, db_name):
        with open("../storage/key.json", "r") as env:
            env_dict = json.load(env)
        sql_db_pw = env_dict["sql_password"]
        conn = pymysql.connect(host='127.0.0.1', user='root', password=sql_db_pw)
        cursor = conn.cursor()
        conn.commit()

        sql = f"CREATE DATABASE IF NOT EXISTS {db_name}"
        cursor.execute(sql)
        conn.close()

    def create_table(self, conn, cursor, table_name, sql_col):
        sql = f"""CREATE TABLE IF NOT EXISTS {table_name} 
        ({sql_col}) ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        cursor.execute(sql)
        conn.commit()

        return conn, cursor

    def get_data_from_table(self, cursor, sql):
        cursor.execute(sql)
        data_sql = cursor.fetchall()

        return data_sql

    def get_last_row(self, cursor, table_name, row):
        # 왜 빈 튜플이 나오지???
        sql_last_col = f"""SELECT {row}, IFNULL({row}, 0) AS {row} FROM {table_name} ORDER BY {row} DESC LIMIT 1;"""
        cursor.execute(sql_last_col)
        last_col = cursor.fetchall()

        return last_col

    def get_col_list(self, cursor, table_name):
        sql_col_list = f"""SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` 
        WHERE `TABLE_SCHEMA`='mu_tech' AND `TABLE_NAME`='{table_name}';"""
        cursor.execute(sql_col_list)
        col_list = cursor.fetchall()

        return col_list

    def insert_data_to_table(self, conn, cursor, list_col, table_sql, tuple_data):
        sql = f"""INSERT INTO {table_sql}({list_col}) VALUES{tuple_data}"""
        cursor.execute(sql)
        conn.commit()

        return conn, cursor


class db:
    def __init__(self, cur, args):
        self.cur = cur
        self.sql = args
        self.search_data()
        self.make_df()

    def search_data(self):
        self.cur.execute(self.sql)
        self.results = self.cur.fetchall()
        return self.results

    def make_df(self):
        import pandas as pd
        self.list = []
        for i in self.results:
            self.list.append(i)
        self.columns = self.sql
        if ' FROM' in self.columns:
            self.columns = self.sql.split(' FROM')
            self.columns = self.columns[0]
            self.columns = self.columns.split(' ')
            self.columns.pop(0)
            self.columns = ''.join(self.columns)
            self.columns = self.columns.replace('DISTINCT', '')
            self.columns = self.columns.split(',')
        else:
            self.columns = self.sql.split(' from')
            self.columns = self.columns[0]
            self.columns = self.columns.split(' ')
            self.columns.pop(0)
            self.columns = ''.join(self.columns)
            self.columns = self.columns.replace('DISTINCT', '')
            self.columns = self.columns.split(',')

        self.dataframe = pd.DataFrame(self.list, columns=self.columns)

        return self.dataframe

    def disconnect(self):
        self.cur.close()
        self.conn.close()
