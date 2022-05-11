from datetime import datetime, timedelta
from pymongo import MongoClient
from data_transformation.db_env import DbEnv, db


conn, cursor = DbEnv().connect_sql()

# == 몽고디비 ==
client = MongoClient('localhost', 27017)
db1 = client.music_cow
db2 = client.article
db3 = client.musicowlab

# music cow
col1 = db1.musicCowData
col2 = db1.mcpi
col3 = db1.copyright_price
col4 = db1.musicInfo
col7 = db1.newsLink

# article
col5 = db2.article_info
col6 = db2.article_info_history

# web
col8 = db3.mcpi_info
col9 = db3.song_info
col10 = db3.index_rank

dateToday = datetime.today().strftime('%Y-%m-%d')
dateYesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')


def update_mcpi_info():
    list_mcpi = list(col7.find({'artist': '뮤직카우', 'date': dateToday}))
    if len(list_mcpi) == 0:
        list_mcpi = list(col7.find({'artist': '뮤직카우', 'date': dateYesterday}))
        for dict_nlp in list_mcpi:
            del dict_nlp['_id'], dict_nlp['artist'], dict_nlp['song_num'], dict_nlp['date']
    else:
        for dict_nlp in list_mcpi:
            del dict_nlp['_id'], dict_nlp['artist'], dict_nlp['song_num'], dict_nlp['date']

    dict_mcpi = {}
    sql = "SELECT fng from mu_tech.dailyfng WHERE date='%s' and num=0" % dateYesterday
    df_fng = db(cursor, sql).dataframe

    sql = "SELECT price, volume from mu_tech.dailymcpi WHERE date='%s'" % dateYesterday
    df_mcpi = db(cursor, sql).dataframe

    dict_mcpi['nlp'], dict_mcpi['date'], dict_mcpi['fng'], dict_mcpi['price'], dict_mcpi['volume'] = \
        dict_nlp, dateYesterday, df_fng['fng'][0], df_mcpi['price'][0], int(df_mcpi['volume'][0])

    col8.insert_one(dict_mcpi).inserted_id

def update_song_info(dict_song):
    del dict_song['_id'], dict_song['auc1_info'], dict_song['auc2_info']

    sql = "SELECT price_close, volume from mu_tech.musiccowdata WHERE date='%s' and num=%s" % (dateYesterday, dict_song['num'])
    df_song = db(cursor, sql).dataframe

    sql = "SELECT fng, ranking from mu_tech.dailyfng WHERE date='%s' and num=%s" % (dateYesterday, dict_song['num'])
    df_fng = db(cursor, sql).dataframe
    try:
        dict_song['fng'], dict_song['fng_rank'] = float(df_fng['fng'][0]), int(df_fng['ranking'][0])
    except:
        dict_song['fng'], dict_song['fng_rank'] = 'nan', 'nan'

    sql = "SELECT beta, ranking from mu_tech.dailybeta WHERE date='%s' and num=%s" % (dateYesterday, dict_song['num'])
    df_beta = db(cursor, sql).dataframe
    try:
        dict_song['beta'], dict_song['beta_rank'] = float(df_beta['beta'][0]), int(df_beta['ranking'][0])
    except:
        dict_song['beta'], dict_song['beta_rank'] = 'nan', 'nan'

    sql = "SELECT cap, ranking from mu_tech.dailymarketcap WHERE date='%s' and num=%s" % (dateYesterday, dict_song['num'])
    df_marketcap = db(cursor, sql).dataframe
    try:
        dict_song['marketcap'], dict_song['marketcap_rank'] = int(df_marketcap['cap'][0]), int(df_marketcap['ranking'][0])
    except:
        dict_song['marketcap'], dict_song['marketcap_rank'] = 'nan', 'nan'

    sql = "SELECT tno, ranking from mu_tech.dailyturnover WHERE date='%s' and num=%s" % (dateYesterday, dict_song['num'])
    df_turnover = db(cursor, sql).dataframe
    try:
        dict_song['turnover'], dict_song['turnover_rank'] = float(df_turnover['tno'][0]), int(df_turnover['ranking'][0])
    except:
        dict_song['turnover'], dict_song['turnover_rank'] = 'nan', 'nan'

    sql = "SELECT per, ranking from mu_tech.dailyper WHERE date='%s' and num=%s" % (dateYesterday, dict_song['num'])
    df_per = db(cursor, sql).dataframe
    try:
        dict_song['per'], dict_song['per_rank'] = float(df_per['per'][0]), int(df_per['ranking'][0])
    except:
        dict_song['per'], dict_song['per_rank'] = 'nan', 'nan'

    dict_song['date'], dict_song['price'], dict_song['volume'] = dateYesterday, int(df_song['price_close'][0]), int(df_song['volume'][0]),

    col9.insert_one(dict_song).inserted_id

def update_song_info_nlp(doc):
    list_num, artist_html = doc['song_num'], doc['artist']
    del doc['_id'], doc['artist'], doc['song_num'], doc['date']

    for x in list_num:
        col9.update_one({'num': x, 'date': dateYesterday}, {'$set': {'nlp': doc, 'nlp_html': artist_html}})

def update_rank_info():
    sql = "SELECT num, fng, ranking from mu_tech.dailyfng WHERE date='%s' and num NOT IN (0) ORDER BY ranking ASC" % dateYesterday
    df_fng = db(cursor, sql).dataframe
    df_fng['ranking'] = df_fng['ranking'].apply(str)
    dict_fng = df_fng.set_index('ranking').T.to_dict()
    dict_fng['type'], dict_fng['date'] = 'fng', dateYesterday

    sql = "SELECT num, beta, ranking from mu_tech.dailybeta WHERE date='%s' ORDER BY ranking ASC" % dateYesterday
    df_beta = db(cursor, sql).dataframe
    df_beta['ranking'] = df_beta['ranking'].apply(str)
    dict_beta = df_beta.set_index('ranking').T.to_dict()
    dict_beta['type'], dict_beta['date'] = 'beta', dateYesterday

    sql = "SELECT num, cap, ranking from mu_tech.dailymarketcap WHERE date='%s' ORDER BY ranking ASC" % dateYesterday
    df_marketcap = db(cursor, sql).dataframe
    df_marketcap['ranking'] = df_marketcap['ranking'].apply(str)
    dict_marketcap = df_marketcap.set_index('ranking').T.to_dict()
    dict_marketcap['type'], dict_marketcap['date'] = 'marketcap', dateYesterday

    sql = "SELECT num, tno, ranking from mu_tech.dailyturnover WHERE date='%s' ORDER BY ranking ASC" % dateYesterday
    df_turnover = db(cursor, sql).dataframe
    df_turnover['ranking'] = df_turnover['ranking'].apply(str)
    dict_turnover = df_turnover.set_index('ranking').T.to_dict()
    dict_turnover['type'], dict_turnover['date'] = 'turnover', dateYesterday

    sql = "SELECT num, per, ranking from mu_tech.dailyper WHERE date='%s' ORDER BY ranking ASC" % dateYesterday
    df_per = db(cursor, sql).dataframe
    df_per['ranking'] = df_per['ranking'].apply(str)
    dict_per = df_per.set_index('ranking').T.to_dict()
    dict_per['type'], dict_per['date'] = 'per', dateYesterday

    col10.insert_one(dict_fng).inserted_id
    col10.insert_one(dict_beta).inserted_id
    col10.insert_one(dict_marketcap).inserted_id
    col10.insert_one(dict_turnover).inserted_id
    col10.insert_one(dict_per).inserted_id

