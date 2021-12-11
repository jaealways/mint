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
        sql = f"""CREATE TABLE IF NOT EXISTS {table_name} """
        cursor.execute(sql)

    def get_last_row(self, conn, cursor, table_name, row):
        sql = f"""SELECT fields FROM {table_name} ORDER BY {row} DECS LIMIT 1"""
        cursor.execute((sql % (row)))
        conn.commit()

        return conn
