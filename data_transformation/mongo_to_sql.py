from db_env import DbEnv
from pymongo import MongoClient
from tqdm import tqdm

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

    def create_table_daily_mcpi(self):
        conn_sql, cursor_sql = DbEnv().connect_sql()
        sql_col = """date varchar(255) NOT NULL,
        price int(11) NOT NULL,
        volume int(11) NOT NULL"""
        DbEnv().create_table(conn_sql, cursor_sql, 'dailyMCPI', sql_col)

    def create_table_news_token(self, conn, cursor):
        sql_col = """token varchar(10000) NOT NULL,
        tag varchar(10000) NOT NULL,
        doc_num varchar(255) NOT NULL,
        artist varchar(255) NOT NULL,
        date varchar(255) NOT NULL,
        date_crawler varchar(255) NOT NULL"""
        DbEnv().create_table(conn, cursor, 'newstoken', sql_col)

    def create_table_news_sen_token(self, conn, cursor):
        sql_col = """token TEXT NOT NULL,
        tag TEXT NOT NULL,
        doc_num varchar(255) NOT NULL,
        artist varchar(255) NOT NULL,
        date varchar(255) NOT NULL,
        date_crawler varchar(255) NOT NULL"""
        DbEnv().create_table(conn, cursor, 'newssentoken', sql_col)

    def create_table_news_sen(self, conn, cursor):
        sql_col = """sen varchar(10000) NOT NULL,
        doc_num varchar(255) NOT NULL,
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

    def update_daily_music_cow(self):
        list_mongo = col1.find({})
        for mongo_song in tqdm(list_mongo):
            sql = """SELECT date FROM mu_tech.musiccowdata WHERE num = '%s'""" % mongo_song['num']
            cursor.execute(sql)
            tuple_sql = cursor.fetchall()
            set_sql = set([x[0] for x in tuple_sql])
            set_mongo = set(mongo_song.keys()) - set(['_id', 'num', 'song_title', 'song_artist'])
            list_insert = list(set_mongo - set_sql)

            for key_date in list_insert:
                set_mongo_song = mongo_song[key_date]
                tuple_insert = (mongo_song['num'], key_date, int(set_mongo_song['price_high']), int(set_mongo_song['price_low']),
                                int(set_mongo_song['price_close']), float(set_mongo_song['pct_price_change']), int(set_mongo_song['cnt_units_traded']))

                sql = f"""INSERT INTO mu_tech.musiccowdata (num, date, price_high, price_low, price_close, price_ratio, volume)
                 VALUES {tuple_insert}"""
                cursor.execute(sql)
                conn.commit()

    def update_daily_mcpi(self):
        list_mongo = col2.find({})
        for mongo_mcpi in list_mongo:
            sql = """SELECT date FROM mu_tech.dailyMCPI"""
            cursor.execute(sql)
            tuple_sql = cursor.fetchall()
            set_sql = set([x[0] for x in tuple_sql])
            set_mongo = set(mongo_mcpi.keys()) - set(['_id'])
            list_insert = list(set_mongo - set_sql)

            for key_date in list_insert:
                sql = """SELECT volume FROM mu_tech.musiccowdata WHERE date = '%s'""" % key_date
                cursor.execute(sql)
                tuple_sql = cursor.fetchall()
                list_sql = [x[0] for x in tuple_sql]
                sum_sql = sum(list_sql)

                set_mongo_song = mongo_mcpi[key_date]
                tuple_insert = (key_date, float(set_mongo_song), int(sum_sql))

                sql = f"""INSERT INTO mu_tech.dailyMCPI (date, price, volume) VALUES {tuple_insert}"""
                cursor.execute(sql)
                conn.commit()


# daily routine
conn, cursor = DbEnv().connect_sql()

# == 몽고디비 ==
client = MongoClient('localhost', 27017)
db1 = client.music_cow
db2 = client.article

# music cow
col1 = db1.musicCowData
col2 = db1.mcpi
col3 = db1.copyright_price
col4 = db1.musicInfo

# article
col5 = db2.article_info

if __name__ == '__main__':
    mongo_sql = MongoToSQL()

    mongo_sql.update_daily_mcpi()



