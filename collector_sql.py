from sqlalchemy import create_engine
import pymysql
print("collector_sql 시작")

pymysql.install_as_MYSQLdb()

class Collector:
    def __init__(self):
        self.engine_bot = None

    def db_setting(self, db_name, db_id, db_passwd, db_ip, db_port):
        self.engine_bot = create_engine("mysql+mysqldb://" + db_id + ":" + db_passwd + "@"
                                        + db_ip + ":" + db_port + "/" + db_name, encoding = 'utf-8')

        self.collector

    # def sql_collecting(self):
    #     sql = "select "

print("collector_sql.py의 __name__:", __name__)

if __name__ == "__main__":
    print("__main__에 들어옴")
    c = Collector()
    db_name = 'music_cow'
    db_id = 'kuggle2'
    db_ip = ''
    db_passwd = 'abc123456789'
    db_port = '3306'

    sql = "select * from bot_test1.class1;"

    rows = c.engine_bot.execute(sql).fetchall()
    print(rows)