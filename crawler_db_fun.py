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
    def db_create_value(self, db, col, dict_list=None):
        db_in = client['%s' % db]['%s' % col].insert_one(dict_list).inserted_id
        print(db_in)
        return db_in

    def db_add_value(self, db, col, dict_list=None, val_name=None, num=None):
        """dict_list: 기존 document와 통합하려는 dict
        val_name: 검색을 통해 리스트를 매칭하는 과정에서 기준이 되는 변수 이름
        num: val_name을 통해 실제로 매칭을 원하는 값"""
        dict_in_db = dict({'%s' % val_name: num}, **dict_list)
        db_in = client['%s' % db]['%s' % col].insert_one(dict_in_db).inserted_id
        print(db_in)
        return db_in
