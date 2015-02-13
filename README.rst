
SUMMARY
-------

An attempt to parse SQL and generate meaningful MongoDB query expressions.

UNWIND EXAMPLE
--------------

> db.sidtest.insert({type:"new", vals:[{orders:4, trades:2, min:1}, {orders:8, trades:4, min:2}]})
WriteResult({ "nInserted" : 1 })
> db.sidtest.insert({type:"new", vals:[{orders:14, trades:12, min:11}, {orders:18, trades:14, min:12}]})
WriteResult({ "nInserted" : 1 })

> db.sidtest.aggregate([{"$match":{type:'new'}}, {"$unwind": '$vals'}, {"$project": {type:1, orders:'$vals.orders'}}])
{ "_id" : ObjectId("54dc2a4c53f2873d69794326"), "type" : "new", "orders" : 4 }
{ "_id" : ObjectId("54dc2a4c53f2873d69794326"), "type" : "new", "orders" : 8 }
{ "_id" : ObjectId("54dc2a5d53f2873d69794327"), "type" : "new", "orders" : 14 }
{ "_id" : ObjectId("54dc2a5d53f2873d69794327"), "type" : "new", "orders" : 18 }

> db.sidtest.aggregate([{"$match":{type:'new'}}, {"$project": {type:1, orders:'$vals.orders'}}])
{ "_id" : ObjectId("54dc2a4c53f2873d69794326"), "type" : "new", "orders" : [ 4, 8 ] }
{ "_id" : ObjectId("54dc2a5d53f2873d69794327"), "type" : "new", "orders" : [ 14, 18 ] }
>


Assuming the collection structure below:

{ "_id" : ObjectId("54dc2a4c53f2873d69794326"), "type" : "new", "otherfld": 'whatever1', "vals" : [ { "orders" : 4, "trades" : 2, "min" : 1 }, { "orders" : 8, "trades" : 4, "min" : 2 } ] }
{ "_id" : ObjectId("54dc2a5d53f2873d69794327"), "type" : "new", "otherfld": 'whatever2', "vals" : [ { "orders" : 14, "trades" : 12, "min" : 11 }, { "orders" : 18, "trades" : 14, "min" : 12 } ] }

- SELECT type, vals from SIDTEST
  - { "_id" : ObjectId("54dc2a4c53f2873d69794326"), "type" : "new", "vals" : [ { "orders" : 4, "trades" : 2, "min" : 1 }, { "orders" : 8, "trades" : 4, "min" : 2 } ] }
  - { "_id" : ObjectId("54dc2a4c53f2873d69794326"), "type" : "new", "vals" : [ { "orders" : 4, "trades" : 2, "min" : 1 }, { "orders" : 8, "trades" : 4, "min" : 2 } ] }

- SELECT type, vals.orders from SIDTEST
  - { "_id" : ObjectId("54dc2a4c53f2873d69794326"), "type" : "new", "vals" : [ { "orders" : 4 }, { "orders" : 8} ] }
  - { "_id" : ObjectId("54dc2a4c53f2873d69794326"), "type" : "new", "vals" : [ { "orders" : 4, "trades" : 2, "min" : 1 }, { "orders" : 8, "trades" : 4, "min" : 2 } ] }

- SELECT type, SUM(vals.orders) from SIDTEST
 - db.sidtest.aggregate([{"$match":{type:'new'}}, {"$unwind": "$vals"}, {"$group": {_id: null, 'orders': {'$sum': '$vals.orders'} } }])


Slightly different schema
-------------------------

db.sidtest.insert({userid:"sid", metric: 'latency', vals:[{latency1:1, latency2:2, min:1}, {latency1:1, latency2:2, min:3}]})

> db.sidtest.find()
{ "_id" : ObjectId("54dd7efb53f2873d69794328"), "userid" : "sid", "metric" : "latency", "vals" : [ { "latency1" : 1, "latency2" : 2, "min" : 1 }, { "latency1" : 1, "latency2" : 2, "min" : 3 } ] }

{ "_id" : ObjectId("54dd7f1153f2873d69794329"), "userid" : "sid", "metric" : "latency", "vals" : [ { "latency1" : 11, "latency2" : 12, "min" : 1 }, { "latency1" : 2, "latency2" : 4, "min" : 3 } ] }

{ "_id" : ObjectId("54dd801f53f2873d6979432a"), "userid" : "sid", "metric" : "latency", "vals" : [ { "latency1" : 11, "latency2" : 12, "min" : 1 }, { "latency1" : 2, "latency2" : 4, "min" : 3 } ], "vals2" : [ 22, 23, 24 ] }

{ "_id" : ObjectId("54dd802553f2873d6979432b"), "userid" : "sid", "metric" : "latency", "vals" : [ { "latency1" : 11, "latency2" : 12, "min" : 1 }, { "latency1" : 2, "latency2" : 4, "min" : 3 } ], "vals2" : [ 32, 33, 34 ] }\

> db.sidtest.aggregate([{'$match':{userid: 'sid'}}, {'$project': {latency1: '$vals.latency1'}}])
{ "_id" : ObjectId("54dd7efb53f2873d69794328"), "latency1" : [ 1, 1 ] }
{ "_id" : ObjectId("54dd7f1153f2873d69794329"), "latency1" : [ 11, 2 ] }
{ "_id" : ObjectId("54dd801f53f2873d6979432a"), "latency1" : [ 11, 2 ] }
{ "_id" : ObjectId("54dd802553f2873d6979432b"), "latency1" : [ 11, 2 ] }
> db.sidtest.aggregate([{'$match':{userid: 'sid'}}, {'$project': {latency1: '$vals.latency1', vals2: '$vals2'}}])
{ "_id" : ObjectId("54dd7efb53f2873d69794328"), "latency1" : [ 1, 1 ] }
{ "_id" : ObjectId("54dd7f1153f2873d69794329"), "latency1" : [ 11, 2 ] }
{ "_id" : ObjectId("54dd801f53f2873d6979432a"), "vals2" : [ 22, 23, 24 ], "latency1" : [ 11, 2 ] }
{ "_id" : ObjectId("54dd802553f2873d6979432b"), "vals2" : [ 32, 33, 34 ], "latency1" : [ 11, 2 ] }
> db.sidtest.aggregate([{'$match':{userid: 'sid'}}, {'$unwind': '$vals2'}, {'$project': {latency1: '$vals.latency1', vals2: '$vals2'}}])
{ "_id" : ObjectId("54dd801f53f2873d6979432a"), "vals2" : 22, "latency1" : [ 11, 2 ] }
{ "_id" : ObjectId("54dd801f53f2873d6979432a"), "vals2" : 23, "latency1" : [ 11, 2 ] }
{ "_id" : ObjectId("54dd801f53f2873d6979432a"), "vals2" : 24, "latency1" : [ 11, 2 ] }
{ "_id" : ObjectId("54dd802553f2873d6979432b"), "vals2" : 32, "latency1" : [ 11, 2 ] }
{ "_id" : ObjectId("54dd802553f2873d6979432b"), "vals2" : 33, "latency1" : [ 11, 2 ] }
{ "_id" : ObjectId("54dd802553f2873d6979432b"), "vals2" : 34, "latency1" : [ 11, 2 ] }
> db.sidtest.aggregate([{'$match':{userid: 'sid'}}, {'$unwind': '$vals2'}, {'$unwind': '$vals'}, {'$project': {latency1: '$vals.latency1', vals2: '$vals2'}}])
{ "_id" : ObjectId("54dd801f53f2873d6979432a"), "vals2" : 22, "latency1" : 11 }
{ "_id" : ObjectId("54dd801f53f2873d6979432a"), "vals2" : 22, "latency1" : 2 }
{ "_id" : ObjectId("54dd801f53f2873d6979432a"), "vals2" : 23, "latency1" : 11 }
{ "_id" : ObjectId("54dd801f53f2873d6979432a"), "vals2" : 23, "latency1" : 2 }
{ "_id" : ObjectId("54dd801f53f2873d6979432a"), "vals2" : 24, "latency1" : 11 }
{ "_id" : ObjectId("54dd801f53f2873d6979432a"), "vals2" : 24, "latency1" : 2 }
{ "_id" : ObjectId("54dd802553f2873d6979432b"), "vals2" : 32, "latency1" : 11 }
{ "_id" : ObjectId("54dd802553f2873d6979432b"), "vals2" : 32, "latency1" : 2 }
{ "_id" : ObjectId("54dd802553f2873d6979432b"), "vals2" : 33, "latency1" : 11 }
{ "_id" : ObjectId("54dd802553f2873d6979432b"), "vals2" : 33, "latency1" : 2 }
{ "_id" : ObjectId("54dd802553f2873d6979432b"), "vals2" : 34, "latency1" : 11 }
{ "_id" : ObjectId("54dd802553f2873d6979432b"), "vals2" : 34, "latency1" : 2 }
> db.sidtest.aggregate([{'$match':{userid: 'sid'}}, {'$unwind': '$vals'}, {'$unwind': '$vals2'}, {'$project': {latency1: '$vals.latency1', vals2: '$vals2'}}])
{ "_id" : ObjectId("54dd801f53f2873d6979432a"), "vals2" : 22, "latency1" : 11 }
{ "_id" : ObjectId("54dd801f53f2873d6979432a"), "vals2" : 23, "latency1" : 11 }
{ "_id" : ObjectId("54dd801f53f2873d6979432a"), "vals2" : 24, "latency1" : 11 }
{ "_id" : ObjectId("54dd801f53f2873d6979432a"), "vals2" : 22, "latency1" : 2 }
{ "_id" : ObjectId("54dd801f53f2873d6979432a"), "vals2" : 23, "latency1" : 2 }
{ "_id" : ObjectId("54dd801f53f2873d6979432a"), "vals2" : 24, "latency1" : 2 }
{ "_id" : ObjectId("54dd802553f2873d6979432b"), "vals2" : 32, "latency1" : 11 }
{ "_id" : ObjectId("54dd802553f2873d6979432b"), "vals2" : 33, "latency1" : 11 }
{ "_id" : ObjectId("54dd802553f2873d6979432b"), "vals2" : 34, "latency1" : 11 }
{ "_id" : ObjectId("54dd802553f2873d6979432b"), "vals2" : 32, "latency1" : 2 }
{ "_id" : ObjectId("54dd802553f2873d6979432b"), "vals2" : 33, "latency1" : 2 }
{ "_id" : ObjectId("54dd802553f2873d6979432b"), "vals2" : 34, "latency1" : 2 }

LIMITATIONS
-----------

- Array or object names cannot be SQL keywords such as "VALUES"

DEPENDS ON:
----------

sqlparse: https://github.com/andialbrecht/sqlparse

