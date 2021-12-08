from pymongo import MongoClient
import pymysql
import json

# mongodb 저장 형식을 sql 형식으로 옮기기, 연산처리 빠르게

class DbEnv:
    def mongo_connect(self, db, col):
        client = MongoClient('localhost', 27017)
        conn = client['%s' % db]['%s' % col]

        return conn

    def sql_connect(self):
        with open("../storage/key.json", "r") as env:
            env_dict = json.load(env)
        sql_db_pw = env_dict["sql_password"]
        conn = pymysql.connect(host='127.0.0.1', user='root', password=sql_db_pw, db='mu_tech', charset='utf8')
        cursor = conn.cursor()
        conn.commit()

        return cursor, conn

    def create_db(self, conn):
        try:
            with conn.cursor() as cursor:
                sql = '''CREATE DATABASE mu_tech'''
                cursor.execute(sql)
            conn.commit()
        finally:
            conn.close()
        cursor = conn.cursor()
        conn.commit()

        return conn, cursor

    def create_table(self, conn):
        try:
            with conn.cursor() as cursor:
                sql = '''CREATE TABLE IF NOT EXISTS users (
                id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
                email varchar(255) NOT NULL,
                password varchar(255) NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8'''
                cursor.execute(sql)
            conn.commit()
        finally:
            conn.close()
        cursor = conn.cursor()
        conn.commit()

        return conn, cursor
