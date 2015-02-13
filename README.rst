
SUMMARY
-------

An attempt to parse SQL and generate meaningful MongoDB query expressions. The problem this project is trying
to solve is that of providing an intuitive SQL interface for accessing MongoDB data, especially when it involves
denormalized data stored in arrays and objects inside a collection.


Test schema
-----------

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

