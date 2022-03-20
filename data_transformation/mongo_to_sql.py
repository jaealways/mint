from db_env import DbEnv

# mongodb 저장 형식을 sql 형식으로 옮기기, 연산처리 빠르게

class MongoToSQL:
    def create_table_daily_music_cow(self):
        conn_sql, cursor_sql = DbEnv().connect_sql()
        sql_col = """num int(11) NOT NULL,
        date varchar(255) NOT NULL,
        price_high int(11) NOT NULL,
        price_low int(11) NOT NULL,
        price_open int(11) NOT NULL,
        price_close int(11) NOT NULL,
        price_ratio float(11) NOT NULL,
        volume int(11) NOT NULL"""
        DbEnv().create_table(conn_sql, cursor_sql, 'musicCowData', sql_col)

    def create_table_news_token(self, conn, cursor):
        sql_col = """token varchar(10000) NOT NULL,
        tag varchar(10000) NOT NULL,
        artist varchar(255) NOT NULL,
        date varchar(255) NOT NULL,
        date_crawler varchar(255) NOT NULL"""
        DbEnv().create_table(conn, cursor, 'newstoken', sql_col)

    def create_table_news_sen(self, conn, cursor):
        sql_col = """sen varchar(10000) NOT NULL,
        artist varchar(255) NOT NULL,
        date varchar(255) NOT NULL,
        date_crawler varchar(255) NOT NULL"""
        DbEnv().create_table(conn, cursor, 'newsSen', sql_col)
        sql = """ALTER DATABASE mu_tech CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;"""
        cursor.execute(sql)
        sql = """ALTER TABLE newssen CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"""
        cursor.execute(sql)
        # sql = "ALTER TABLE newssen CHANGE sen TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        # cursor.execute(sql)

    def update_daily_music_cow(self, tuple_mongo, tuple_sql):
        tuple_mongo_temp = [(x, y) for x, y, z in tuple_mongo]
        tuple_update = list(set(tuple_mongo_temp) - set(tuple_sql))
        tuple_update.sort()

        # 튜플 수정하기
        tuple_update_idx = [tuple_mongo[idx] for idx, val in enumerate(tuple_mongo_temp) if val in tuple_update]
        tuple_update_idx = []

        DbEnv.insert_data_to_table(self, conn=conn_sql, cursor=cursor_sql, list_col=list_col,
                                   table_sql=table_sql, tuple_data=tuple_data)


    def get_col_mcpi(self):
        conn_sql, cursor_sql = DbEnv().connect_sql()
        sql_col = """date varchar(255) NOT NULL,
        price float(10, 4) NOT NULL"""
        conn_sql, cursor_sql = DbEnv().create_table(conn_sql, cursor_sql, 'daily_mcpi', sql_col)
        col_list = DbEnv().get_col_list(cursor_sql, 'daily_mcpi')
        col_list = [item[0] for item in col_list]
        last_col = DbEnv().get_last_row(cursor_sql, 'daily_mcpi', 'date')
        print(col_list, last_col)

        return last_col, col_list

    def make_tuple_mongo_daily_music_cow(self):
        conn_mongo = DbEnv().connect_mongo('music_cow', 'musicCowData')
        list_num, list_date, list_price, list_temp = [], [], [], []
        list_mongo = list(conn_mongo.find())
        list(map(lambda x: list_num.extend([x['num'] for z in range(len(set(list(x.keys())) - set(['_id', 'num', 'song_title', 'song_artist'])))]), list_mongo))
        list(map(lambda x: list_temp.extend([(k, v) for k, v in x.items() if k not in {'_id', 'num', 'song_title', 'song_artist'}]), list_mongo))
        list_date.extend([k for k, v in list_temp])
        list_price.extend(tuple(list(v.values())) for k, v in list_temp)
        tuple_mongo = list(zip(list_num, list_date, list_price))

        return tuple_mongo

    def make_tuple_sql_daily_music_cow(self):
        conn, cursor = DbEnv().connect_sql()
        sql = "SELECT DISTINCT num, date FROM musicCowData ORDER BY num"
        cursor.execute(sql)
        tuple_sql = list(cursor.fetchall())

        return tuple_sql

    def update_sql_daily_music_cow(self, col_mongo, table_sql):
        conn_mongo = DbEnv().connect_mongo('music_cow', col_mongo)
        conn_sql, cursor_sql = DbEnv().connect_sql()
        dict_col = conn_mongo.find()
        list_col = "num, date, price_high, price_low, price_close, price_ratio, volume"
        for x in dict_col:
            num = x['num']
            for index, (key, elem) in enumerate(x.items()):
                if str(type(elem)) == "<class 'dict'>":
                    if key > str(last_col):
                        tuple_data = (num, key, int(elem['price_high']), int(elem['price_low']), int(elem['price_close']),
                                      float(elem['pct_price_change']), int(elem['cnt_units_traded']))
                        DbEnv.insert_data_to_table(self, conn=conn_sql, cursor=cursor_sql, list_col=list_col,
                                                   table_sql=table_sql, tuple_data=tuple_data)
                        print(key, num, int(elem['price_high']))

    def update_sql_list_song_artist(self, col_mongo, table_sql):
        conn_mongo = DbEnv().connect_mongo('music_cow', col_mongo)
        conn_sql, cursor_sql = DbEnv().connect_sql()
        dict_col = conn_mongo.find()
        list_col = "num, title, artist"
        for x in dict_col:
            num, title, artist = x['num'], x['song_title'], x['song_artist']
            tuple_data = (num, title, artist)
            DbEnv.insert_data_to_table(self, conn=conn_sql, cursor=cursor_sql, list_col=list_col,
                                       table_sql=table_sql, tuple_data=tuple_data)
            print(num, title, artist)

    def update_sql_daily_mcpi(self, col_mongo, table_sql):
        conn_mongo = DbEnv().connect_mongo('music_cow', col_mongo)
        conn_sql, cursor_sql = DbEnv().connect_sql()
        dict_col = conn_mongo.find()
        list_col = "date, price"
        for x in dict_col:
            for index, (key, elem) in enumerate(x.items()):
                if str(type(elem)) != "<class 'bson.objectid.ObjectId'>":
                    if key > str(last_col):
                        tuple_data = (key, elem)
                        DbEnv.insert_data_to_table(self, conn=conn_sql, cursor=cursor_sql, list_col=list_col,
                                                   table_sql=table_sql, tuple_data=tuple_data)
                        print(key, elem)

    def update_sql_daily_youtube(self, col_mongo, table_sql):
        conn_mongo = DbEnv().connect_mongo('music_cow', col_mongo)
        conn_sql, cursor_sql = DbEnv().connect_sql()
        dict_col = conn_mongo.find()
        list_col = "song_num, video_num, date, viewCount, likeCount, dislikeCount, favoriteCount, commentCount"
        for x in dict_col:
            song_num, video_num = x['num'], x['video_num']
            for index, (key, elem) in enumerate(x.items()):
                if str(type(elem)) == "<class 'dict'>":
                    if key > str(last_col):
                        for col in col_list:
                            if col not in elem.keys():
                                elem[col] = 0
                        tuple_data = (song_num, video_num, key, int(elem['viewCount']), int(elem['likeCount']),
                                      int(elem['dislikeCount']), int(elem['favoriteCount']), int(elem['commentCount']))
                        DbEnv.insert_data_to_table(self, conn=conn_sql, cursor=cursor_sql, list_col=list_col,
                                                   table_sql=table_sql, tuple_data=tuple_data)
                        print(song_num, video_num, key, int(elem['viewCount']), int(elem['likeCount']),
                                      int(elem['dislikeCount']), int(elem['favoriteCount']), int(elem['commentCount']))

    def update_sql_daily_genie(self, col_mongo, table_sql, col_list):
        conn_mongo = DbEnv().connect_mongo('music_cow', col_mongo)
        conn_sql, cursor_sql = DbEnv().connect_sql()
        dict_col = conn_mongo.find()
        list_col = "song_id, date, total_listener, total_play, total_like"
        for x in dict_col:
            song_id = x['link'].split('xgnm=')[1]
            for index, (key, elem) in enumerate(x.items()):
                # 각 변수에 맞게 할당하기
                if str(type(elem)) == "<class 'dict'>":
                    if key > str(last_col):
                        for col in col_list:
                            if col not in elem.keys():
                                elem[col] = 0
                        tuple_data = (song_id, key, int(elem['total_listener']), int(elem['total_play']), int(elem['like']))
                        DbEnv.insert_data_to_table(self, conn=conn_sql, cursor=cursor_sql, list_col=list_col,
                                                   table_sql=table_sql, tuple_data=tuple_data)
                        print(song_id, key, int(elem['total_listener']), int(elem['total_play']), int(elem['like']))


# daily routine
mongo_sql = MongoToSQL()

# DbEnv().create_db('mu_tech')
conn, cursor = DbEnv().connect_sql()
mongo_sql.create_table_news_token(conn, cursor)
# mongo_sql.create_table_news_sen(conn, cursor)

tuple_mongo = mongo_sql.make_tuple_mongo_daily_music_cow()
tuple_sql = mongo_sql.make_tuple_sql_daily_music_cow()
mongo_sql.update_daily_music_cow(tuple_mongo, tuple_sql)

# last_col, col_list = mongo_sql.get_col_mcpi()
# mongo_sql.update_sql_daily_mcpi('daily_mcpi', 'daily_mcpi')

# last_col, col_list = mongo_sql.get_col_daily_music_cow()
# mongo_sql.update_sql_daily_music_cow('daily_music_cow', 'daily_music_cow')

# last_col, col_list = mongo_sql.get_col_list_song_artist()
# mongo_sql.update_sql_list_song_artist('daily_music_cow', 'list_song_artist')

# last_col, col_list = mongo_sql.get_col_daily_youtube()
# mongo_sql.update_sql_daily_youtube('daily_youtube', 'daily_youtube', col_list)

# last_col, col_list = mongo_sql.get_col_daily_genie()
# mongo_sql.update_sql_daily_genie('daily_genie', 'daily_genie', col_list)

