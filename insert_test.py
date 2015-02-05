from pymongo import MongoClient
import datetime
import timeit

client = MongoClient('localhost', 27017)
print client
db = client.sidtest
print db
coll = db.sidtest
#print coll
#for row in coll.find():
#    print row

num = 100000
def insert_array_doc():
    coll.remove({})
    doc = {"ccypair" : "ALL","collat" : "INDIAN","date" : datetime.datetime.utcnow(), "tradedate":20150122, "metric": "metricA", }
    doc["vals"] = [{'min': i, 'orders': 0}for i in range(60)]
    for i in range(num):coll.insert(doc.copy())



def insert_dict_doc():
    coll.remove({})
    doc = {"ccypair" : "ALL","collat" : "INDIAN","date" : datetime.datetime.utcnow(), "tradedate":20150122, "metric": "metricA", }
    doc["vals"] = {str(i):{'orders': 0}for i in range(60)}
    for i in range(num):coll.insert(doc.copy())

def bulk_insert_array_doc():
    coll.remove({})
    doc = {"ccypair" : "ALL","collat" : "INDIAN","date" : datetime.datetime.utcnow(), "tradedate":20150122, "metric": "metricA", }
    doc["vals"] = [{'min': i, 'orders': 0}for i in range(60)]
    docs = [doc.copy() for i in range(num)]
    coll.insert(docs)

def bulk_insert_dict_doc():
    coll.remove({})
    doc = {"ccypair" : "ALL","collat" : "INDIAN","date" : datetime.datetime.utcnow(), "tradedate":20150122, "metric": "metricA"}
    doc["vals"] = {str(i):{'orders': 0}for i in range(60)}
    docs = [doc.copy() for i in range(num)]
    coll.insert(docs)


# 1.25 seconds for 100K updates for 59th minute
def update_arr_docs():
    coll.update({}, {"$set": {"vals.1.orders": 2}}, multi=True)

# 1.3 seconds for 100K updates for 59th minute
def update_dict_docs():
    coll.update({}, {"$set": {"vals.59.orders": 30}}, multi=True)






# array

#print timeit.timeit("insert_array_doc()", setup="from __main__ import insert_array_doc", number=1)
#print timeit.timeit("bulk_insert_array_doc()", setup="from __main__ import bulk_insert_array_doc", number=1)
print timeit.timeit("update_arr_docs()", setup="from __main__ import update_arr_docs", number=1)

# dict
#print timeit.timeit("insert_dict_doc()", setup="from __main__ import insert_dict_doc", number=1)
#print timeit.timeit("bulk_insert_dict_doc()", setup="from __main__ import bulk_insert_dict_doc", number=1)
#print timeit.timeit("update_arr_docs()", setup="from __main__ import update_arr_docs", number=1)





