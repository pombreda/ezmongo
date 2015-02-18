
SUMMARY
-------

An attempt to parse SQL and generate meaningful MongoDB query expressions. The problem this project is trying
to solve is that of providing an intuitive SQL interface for accessing MongoDB data, especially when it involves
denormalized data stored in arrays and objects inside a collection.


The Basic Idea
--------------

The idea is to define a schema (see testconf.py) corresponding to the expected structure of a collection. The schema
doesn't need to have all the possible/expected fields but will mainly be used for figuring how to treat any
compound elements (such as arrays or array of objects).

The problem that is being attempted to be solved here is that of easily making simple SQL queries against
MongoDB collections. SQL is converted into a MongoDB Aggregation Pipeline. In most cases this is straight-forward
but it gets tricky when we have arrays. For example, when storing time series data in documents where each
document contains an array "vals" which has an object for a second (or a minute). Aggregation over such schemata
requires a better understanding of the MongoDB aggregation pipeline (including the $unwind operator).
This project tries to provide an easy SQL interface for such schemata, in order to avoid an adoption barrier as well as
a learning curve for those who haven't mastered the MongoDB query language/aggregation pipeline.


LIMITATIONS
-----------

- Array or object names cannot be SQL keywords such as "VALUES"

DEPENDS ON:
----------

sqlparse: https://github.com/andialbrecht/sqlparse

