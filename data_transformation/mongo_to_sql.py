from pymongo import MongoClient
import pymysql

# mongodb 저장 형식을 sql 형식으로 옮기기, 연산처리 빠르게

class MongoToSQL:
    def __init__(self):
        self.create_db()

    def create_db(self):
        with open("../env.json", "r") as env:
            env_dict = json.load(env)
        sql_db_pw = env_dict["sql_password"]
        conn = pymysql.connect(host='127.0.0.1', user='root', password=sql_db_pw, db='mu_tech', charset='utf8')

        try:
            with conn.cursor() as cursor:
                sql = 'CREATE DATABASE mu_tech'
                cursor.execute(sql)
            conn.commit()
        finally:
            conn.close()

        cursor = conn.cursor()
        conn.commit()


        
