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
        with open("../storage/key.json", "r") as env:
            env_dict = json.load(env)
        sql_db_pw = env_dict["sql_password"]
        conn = pymysql.connect(host='127.0.0.1', user='root', password=sql_db_pw, db='mu_tech', charset='utf8')
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

    def get_data_from_table(self, cursor, row, table_name, where_con):
        sql_last_col = f"""SELECT {row} FROM {table_name} WHERE {where_con};"""
        cursor.execute(sql_last_col)
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

