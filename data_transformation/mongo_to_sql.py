from db_env import DbEnv

from pymongo import MongoClient
import pymysql
import json

# mongodb 저장 형식을 sql 형식으로 옮기기, 연산처리 빠르게

class MongoToSQL:
    def sql_music_list(self):
        conn = DbEnv().mongo_connect('music_cow', 'daily_music_cow')
        dict_col = conn.find({}, {'num': 1, '_id': 0})
        list_col = []

        for x in dict_col:
            k = x.keys()
            v = x[k]
            list_col.append(v)
        # list_col.sort()
        print(list_col)


MongoToSQL().sql_music_list()

