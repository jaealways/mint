from pymongo import MongoClient

client = MongoClient('localhost', 27017)


class DBRead:
    # def db_exist(self):
    #     db_name = ['music_cow', 'daily_crawler', 'music_cow_back_up']
    #     list_col = dict((db, [collection for collection in client[db].list_collection_names()])
    #                    for db in db_name)
    # #     print(list_col)

    def db_read_value(self, db, col):
        read_list = client['%s' % db]['%s' % col].find({})
        return read_list

    def db_check_exist(self, db=None, col=None, val=None, iter_num=0):
        num_check = client['%s' % db]['%s' % col].count_documents({'%s' % val: iter_num})
        to_check_list = client['%s' % db]['%s' % col].find({'%s' % val: iter_num}, {'_id': 0, '%s' % val: 1})
        return num_check, to_check_list


# 1. DB가 존재하는가
# 2. DB에 해당값이 이미 존재하는가
# 3. DB 값 읽어오기
# 4.


class DBWrite:
    def db_write_value(self, db, col, dict_list=None, num=0):
        dict_in_db = dict({'num': num}, **dict_list)
        db_in = client['%s' % db]['%s' % col].insert_one(dict_in_db).inserted_id
        print(db_in)
        return db_in
