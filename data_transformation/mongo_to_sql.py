from db_env import DbEnv

# mongodb 저장 형식을 sql 형식으로 옮기기, 연산처리 빠르게

class MongoToSQL:
    def get_col_daily_music_cow(self):
        conn_sql, cursor_sql = DbEnv().connect_sql()
        sql_col = """num int(11) NOT NULL,
        date varchar(255) NOT NULL,
        price_high int(11) NOT NULL,
        price_low int(11) NOT NULL,
        price_close int(11) NOT NULL,
        price_ratio float(11) NOT NULL,
        volume int(11) NOT NULL"""
        conn_sql, cursor_sql = DbEnv().create_table(conn_sql, cursor_sql, 'daily_music_cow', sql_col)
        col_list = DbEnv().get_col_list(cursor_sql, 'daily_music_cow')
        col_list = [item[0] for item in col_list]
        last_col = DbEnv().get_last_row(cursor_sql, 'daily_music_cow', 'date')
        print(col_list, last_col)

        return last_col, col_list

    def get_col_list_song_artist(self):
        conn_sql, cursor_sql = DbEnv().connect_sql()
        sql_col = """num int(11) NOT NULL,
        title varchar(255) NOT NULL,
        artist varchar(255) NOT NULL"""
        conn_sql, cursor_sql = DbEnv().create_table(conn_sql, cursor_sql, 'list_song_artist', sql_col)
        col_list = DbEnv().get_col_list(cursor_sql, 'list_song_artist')
        col_list = [item[0] for item in col_list]
        last_col = DbEnv().get_last_row(cursor_sql, 'list_song_artist', 'num')
        print(col_list, last_col)

        return last_col, col_list


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

    def get_col_daily_youtube(self):
        conn_sql, cursor_sql = DbEnv().connect_sql()
        sql_col = """song_num int(11) NOT NULL,
        video_num int(11) NOT NULL,
        date varchar(255) NOT NULL,
        viewCount int(11) NOT NULL,
        likeCount int(11) NOT NULL,
        dislikeCount int(11) NOT NULL,
        favoriteCount int(11) NOT NULL,
        commentCount int(11) NOT NULL"""
        conn_sql, cursor_sql = DbEnv().create_table(conn_sql, cursor_sql, 'daily_youtube', sql_col)
        col_list = DbEnv().get_col_list(cursor_sql, 'daily_youtube')
        col_list = [item[0] for item in col_list]
        last_col = DbEnv().get_last_row(cursor_sql, 'daily_youtube', 'date')
        print(col_list, last_col)

        return last_col, col_list

    def get_col_daily_genie(self):
        conn_sql, cursor_sql = DbEnv().connect_sql()
        sql_col = """song_id int(11) NOT NULL,
        date varchar(255) NOT NULL,
        total_listener int(11) NOT NULL,
        total_play int(11) NOT NULL,
        total_like int(11) NOT NULL"""
        conn_sql, cursor_sql = DbEnv().create_table(conn_sql, cursor_sql, 'daily_genie', sql_col)
        col_list = DbEnv().get_col_list(cursor_sql, 'daily_genie')
        col_list = [item[0] for item in col_list]
        last_col = DbEnv().get_last_row(cursor_sql, 'daily_genie', 'date')
        print(col_list, last_col)

        return last_col, col_list

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

DbEnv().create_db('mu_tech')


# last_col, col_list = mongo_sql.get_col_mcpi()
# mongo_sql.update_sql_daily_mcpi('daily_mcpi', 'daily_mcpi')


# last_col, col_list = mongo_sql.get_col_daily_music_cow()
# mongo_sql.update_sql_daily_music_cow('daily_music_cow', 'daily_music_cow')

last_col, col_list = mongo_sql.get_col_list_song_artist()
mongo_sql.update_sql_list_song_artist('daily_music_cow', 'list_song_artist')


# last_col, col_list = mongo_sql.get_col_daily_youtube()
# mongo_sql.update_sql_daily_youtube('daily_youtube', 'daily_youtube', col_list)

# last_col, col_list = mongo_sql.get_col_daily_genie()
# mongo_sql.update_sql_daily_genie('daily_genie', 'daily_genie', col_list)

