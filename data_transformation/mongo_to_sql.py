from data_transformation.db_env import DbEnv
from pymongo import MongoClient
from tqdm import tqdm
import os
import codecs
from datetime import datetime

from data_crawling.artist_for_nlp import df_nlp


class MongoToSQL:
    def create_table_daily_music_cow(self):
        conn_sql, cursor_sql = DbEnv().connect_sql()
        sql_col = """num int(11) NOT NULL,
        date varchar(255) NOT NULL,
        price_high int(11) NOT NULL,
        price_low int(11) NOT NULL,
        price_close int(11) NOT NULL,
        price_ratio float(11) NOT NULL,
        volume int(11) NOT NULL"""
        DbEnv().create_table(conn_sql, cursor_sql, 'musiccowdata', sql_col)

    def create_table_daily_mcpi(self):
        conn_sql, cursor_sql = DbEnv().connect_sql()
        sql_col = """date varchar(255) NOT NULL,
        price float(11) NOT NULL,
        volume int(11) NOT NULL"""
        DbEnv().create_table(conn_sql, cursor_sql, 'dailymcpi', sql_col)

    def create_table_beta(self):
        conn_sql, cursor_sql = DbEnv().connect_sql()
        sql_col = """date varchar(255) NOT NULL,
        num int(11) NOT NULL,
        beta float(11) NOT NULL,
        ranking int(11) NOT NULL"""
        DbEnv().create_table(conn_sql, cursor_sql, 'dailybeta', sql_col)

    def create_table_per(self):
        conn_sql, cursor_sql = DbEnv().connect_sql()
        sql_col = """date varchar(255) NOT NULL,
        num int(11) NOT NULL,
        per float(11) NOT NULL,
        ranking int(11) NOT NULL"""
        DbEnv().create_table(conn_sql, cursor_sql, 'dailyper', sql_col)

    def create_table_marketcap(self):
        conn_sql, cursor_sql = DbEnv().connect_sql()
        sql_col = """date varchar(255) NOT NULL,
        num int(11) NOT NULL,
        cap bigint NOT NULL,
        ranking int(11) NOT NULL"""
        DbEnv().create_table(conn_sql, cursor_sql, 'dailymarketcap', sql_col)

    def create_table_fng(self):
        conn_sql, cursor_sql = DbEnv().connect_sql()
        sql_col = """date varchar(255) NOT NULL,
        num int(11) NOT NULL,
        fng float(22) NOT NULL,
        ranking int(11) NOT NULL"""
        DbEnv().create_table(conn_sql, cursor_sql, 'dailyfng', sql_col)

    def create_table_turnover(self):
        conn_sql, cursor_sql = DbEnv().connect_sql()
        sql_col = """date varchar(255) NOT NULL,
        num int(11) NOT NULL,
        tno float(22) NOT NULL,
        ranking int(11) NOT NULL"""
        DbEnv().create_table(conn_sql, cursor_sql, 'dailyturnover', sql_col)

    def create_table_news_token(self):
        sql_col = """token varchar(10000) NOT NULL,
        tag varchar(10000) NOT NULL,
        doc_num varchar(255) NOT NULL,
        artist varchar(255) NOT NULL,
        date varchar(255) NOT NULL,
        date_crawler varchar(255) NOT NULL"""
        DbEnv().create_table(conn, cursor, 'newstoken', sql_col)

    def create_table_news_sen_token(self):
        sql_col = """token TEXT NOT NULL,
        tag TEXT NOT NULL,
        doc_num varchar(255) NOT NULL,
        artist varchar(255) NOT NULL,
        date varchar(255) NOT NULL,
        date_crawler varchar(255) NOT NULL"""
        DbEnv().create_table(conn, cursor, 'newssentoken', sql_col)

    def create_table_list_song(self):
        sql_col = """num int(11) NOT NULL,
        song_name varchar(255) NOT NULL,
        artist varchar(255) NOT NULL,
        song_name_split varchar(255) NOT NULL,
        artist_split varchar(255) NOT NULL,
        artist_nlp varchar(255) NOT NULL,
        genre varchar(255) NOT NULL,
        stock_num int(11) NOT NULL,
        song_release varchar(255) NOT NULL,
        song_link varchar(255) NOT NULL"""
        DbEnv().create_table(conn, cursor, 'listsong', sql_col)

    def create_table_news_sen(self):
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

    def create_table_topic_news(self):
        sql_col = """artist varchar(30) NOT NULL,
        date varchar(255) NOT NULL,
        topicnum varchar(255) NOT NULL,
        link varchar(255) NOT NULL,
        title text NOT NULL,
        date_news varchar(255) NOT NULL"""
        DbEnv().create_table(conn, cursor, 'topicnews', sql_col)

    def create_table_topic_model(self):
        sql_col = """artist varchar(30) NOT NULL,
        date varchar(255) NOT NULL,
        len_news int(11) NOT NULL,
        code_html LONGTEXT NOT NULL"""
        DbEnv().create_table(conn, cursor, 'topicmodel', sql_col)

    def create_table_topic_keyword(self):
        sql_col = """artist varchar(30) NOT NULL,
        topic varchar(11) NOT NULL,
        date varchar(255) NOT NULL,
        keyword varchar(255) NOT NULL"""
        DbEnv().create_table(conn, cursor, 'topickeyword', sql_col)

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

    def update_list_song(self):
        set_mongo = set(col4.find({}).distinct('num'))
        sql = """SELECT num FROM mu_tech.listsong"""
        cursor.execute(sql)
        tuple_sql = cursor.fetchall()
        set_sql = set([x[0] for x in tuple_sql])
        set_insert = set_mongo - set_sql
        for num in tqdm(set_insert):
            list_info = list(col4.find({'num': num}))[0]
            list_data = list(col1.find({'num': num}))[0]
            artist_nlp = df_artist_nlp[df_artist_nlp['music_cow'] == list_data['song_artist']]['nlp_dict'].values[0]

            tuple_insert = (num, list_info['song_title'], list_info['song_artist'], list_data['song_title'], list_data['song_artist'],
                            str(artist_nlp), list_info['genre'], int(list_info['stock_num']), list_info['song_release_date'], list_info['page'])

            sql = f"""INSERT INTO mu_tech.listsong (num, song_name, artist, song_name_split, artist_split, artist_nlp, genre,
            stock_num, song_release, song_link) VALUES {tuple_insert}"""
            cursor.execute(sql)
            conn.commit()

    def update_daily_beta(self, tuple_insert):
        sql = f"""INSERT INTO mu_tech.dailybeta (date, num, beta, ranking) VALUES {tuple_insert}"""
        cursor.execute(sql)
        conn.commit()

    def update_daily_per(self, tuple_insert):
        sql = f"""INSERT INTO mu_tech.dailyper (date, num, per, ranking) VALUES {tuple_insert}"""
        cursor.execute(sql)
        conn.commit()

    def update_daily_marketcap(self, tuple_insert):
        sql = f"""INSERT INTO mu_tech.dailymarketcap (date, num, cap, ranking) VALUES {tuple_insert}"""
        cursor.execute(sql)
        conn.commit()

    def update_daily_fng(self, tuple_insert):
        sql = f"""INSERT INTO mu_tech.dailyfng (date, num, fng, ranking) VALUES {tuple_insert}"""
        cursor.execute(sql)
        conn.commit()

    def update_daily_turnover(self, tuple_insert):
        sql = f"""INSERT INTO mu_tech.dailyturnover (date, num, tno, ranking) VALUES {tuple_insert}"""
        cursor.execute(sql)
        conn.commit()

    def delete_article_3m(self, date_3m):
        sql = """DELETE FROM mu_tech.newssentoken WHERE date < '%s'""" % date_3m
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
df_artist_nlp = df_nlp()


if __name__ == '__main__':
    mongo_sql = MongoToSQL()
    mongo_sql.create_table_marketcap()
    mongo_sql.create_table_per()
    mongo_sql.create_table_beta()
    mongo_sql.create_table_fng()
    mongo_sql.create_table_turnover()
    mongo_sql.create_table_list_song()
    mongo_sql.create_table_topic_news()
    mongo_sql.create_table_topic_model()
    mongo_sql.create_table_topic_keyword()
    mongo_sql.create_table_daily_music_cow()
    mongo_sql.create_table_daily_mcpi()

    #
    # mongo_sql.update_list_song()
    #
    # mongo_sql.update_daily_mcpi()



