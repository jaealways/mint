from db_env import DbEnv

from pymongo import MongoClient
import pymysql
import json

# mongodb 저장 형식을 sql 형식으로 옮기기, 연산처리 빠르게

class MongoToSQL:
    def set_df_music_list(self):
        conn_mongo = DbEnv().connect_mongo('music_cow', 'daily_music_cow')
        conn_sql, cursor_sql = DbEnv().connect_sql()

        #sql 컬럼 어떻게??
        sql_col = """id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
        email varchar(255) NOT NULL,
        password varchar(255) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        last_col = DbEnv().get_last_row(conn_sql, cursor_sql, 'date', sql_col)

        dict_col = conn_mongo.find({'$gte'})

    def update_table_music_list(self):
        conn = DbEnv().connect_mongo('music_cow', 'daily_music_cow')
        dict_col = conn.find({}, {'num': 1, '_id': 0})
        list_col = []

        for x in dict_col:
            v = list(x.values())[0]
            list_col.append(v)
        list_col.sort()
        print(list_col)


# daily routine
mongo_sql = MongoToSQL()

DbEnv().create_db('mu_tech')

mongo_sql.set_df_music_list()
mongo_sql.update_table_music_list()

