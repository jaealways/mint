from pymongo import MongoClient

client = MongoClient('localhost', 27017)


class DBRead:
    def __init__(self):
        self.db_in_exist()

    # def db_exist(self):
    #     db_name = ['music_cow', 'daily_crawler', 'music_cow_back_up']
    #     list_col = dict((db, [collection for collection in client[db].list_collection_names()])
    #                    for db in db_name)
    # #     print(list_col)

    def db_read_value(self, db, col):
        read_list = client['%s' % db]['%s' % col].find({})
        return read_list

    def db_check_exist(self, db, col, num):
        num_check = client['%s' % db]['%s' % col].count_documents({'num': num})
        to_check_list = client['%s' % db]['%s' % col].find({'num': num}, {'_id': 0, 'num': 1})
        return num_check, to_check_list


# 1. DB가 존재하는가
# 2. DB에 해당값이 이미 존재하는가
# 3. DB 값 읽어오기
# 4.


class DBWrite:
    print('test')

#
# DBRead()

# list_col = dict((db, [collection for collection in client[db].list_collection_names()])
#                for db in client.list_database_names())
#
