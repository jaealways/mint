from pymongo import MongoClient

# class DBRead:
#     def __init__(self):
#
#
# class DBWrite:


client = MongoClient('localhost', 27017)
list_col = dict((db, [collection for collection in client[db].list_collection_names()])
               for db in client.list_database_names())
print(list_col)
# db1 = client.music_cow
# db2 = client.daily_crawler
# col1 = db1.music_list
# col2 = db2.music_list