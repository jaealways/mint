from sqlalchemy import create_engine
import pymysql
from a_song_auc_crawler import



print("collector_sql 시작")

pymysql.install_as_MYSQLdb()

class Collector:
    def __init__(self):
        self.engine_bot = None
        self.cow = song_auc_crawler()

    def db_setting(self, db_name, db_id, db_passwd, db_ip, db_port):
        self.engine_bot = create_engine("mysql+mysqldb://" + db_id + ":" + db_passwd + "@"
                                        + db_ip + ":" + db_port + "/" + db_name, encoding = 'utf-8')

        self.collector

    # def sql_collecting(self):
    #     sql = "select "


#저장한거 db에 접속해서 불러오는 함수

c = Collector()
db_name = 'music_cow'
db_id = 'kuggle2'
db_ip = ''
db_passwd = 'kuggle'
db_port = '3306'

sql = "select * from bot_test1.class1;"

c.db_setting(db_name, db_id, db_passwd, db_ip, db_port)

rows = c.engine_bot.execute(sql).fetchall()
print(rows)