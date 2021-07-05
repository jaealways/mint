from pymongo import MongoClient


class DBRead:
    def __init__(self, db_name=None, col_name=None):
        self.client = MongoClient('localhost', 27017)
        # self.db_exist()
        self.db_read_value()
        # self.db_in_value()

    # def db_exist(self):
    #     db_name = ['music_cow', 'daily_crawler', 'music_cow_back_up']
    #     list_col = dict((db, [collection for collection in client[db].list_collection_names()])
    #                    for db in db_name)
    #     print(list_col)

    def db_read_value(self, db_name, col_name):
        read_db = self.client['%s' % db_name]
        read_col = read_db['%s' % col_name]
        read_list = read_col.find({})

        for x in read_list:
            print(x)

    def db_check_value(self):
        x = self.db_read_value()
        print(x)


# 1. DB가 존재하는가
# 2. DB에 해당값이 이미 존재하는가
# 3. DB 값 읽어오기
# 4.


# class DBWrite:


DBRead()

# list_col = dict((db, [collection for collection in client[db].list_collection_names()])
#                for db in client.list_database_names())
#
# print(list_col)
# db1 = client.music_cow
# db2 = client.daily_crawler
# col1 = db1.music_list
# col2 = db2.music_list
