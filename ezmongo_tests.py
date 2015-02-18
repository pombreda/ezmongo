import unittest
from ezmongo_main import SqlToMongo

class EZMongoTests(unittest.TestCase):
    def setUp(self):
        #print "setUP"
        self.schema = {}
        execfile("testconf.py", self.schema)
        # insert test data
        from pymongo import MongoClient
        client = MongoClient('localhost', 27017)
        db = client.sidtest
        coll = db.sidtest
        coll.remove({})
        # vals as an aray of objects
        for i in range(5):
            coll.insert({"userid":"sid", "metric": 'latency', "vals":[{"latency1":i*1, "latency2":i*2, "min":1+i}, {"latency1":i*3, "latency2":i*4, "min":3+i}]})
        # different userid
        for i in range(6, 10):
            coll.insert({"userid":"todd", "metric": 'latency', "vals":[{"latency1":i*1, "latency2":i*2, "min":1+i}, {"latency1":i*3, "latency2":i*4, "min":3+i}]})
        # vals as an array of scalars
        for i in range(5):
            coll.insert({"userid":"sid", "metric": 'cpus', "vals":[1*i, 2*i, 3*i]})


    def tearDown(self):
        self.schema = {}

    def test_basic_select(self):
        sqm = SqlToMongo(self.schema.get("myschema"))
        select, table, conditions, group, order, limit = sqm.convert_to_mongo("select first, last from students where zip=10001 order by orders desc limit 3")
        print select, table, conditions, group, order, limit
        self.assertNotEqual(select.get("last", None), None)
        self.assertNotEqual(select.get("first", None), None)
        self.assertEqual(table, 'students')
        self.assertEqual(conditions.get('zip').get('val'), '10001')
        self.assertEqual(conditions.get('zip').get('op'), '=')
        self.assertEqual(limit, 3)
        self.assertEqual(order.get("orders desc").get('fld'), 'orders desc')



    def test_array_select(self):
        sqm = SqlToMongo(self.schema.get("myschema"))
        select, table, conditions, group, order, limit = sqm.convert_to_mongo(
            "select user, vals.orders from sidtest where tradedate=20150205 and metric='qd' order by orders desc limit 3")
        print select, table, conditions, group, order, limit
        self.assertEqual(select.get("user", None).get('fld'), 'user')
        self.assertEqual(select.get("vals.orders", None).get('fld'), 'vals.orders')
        self.assertEqual(conditions.get('metric').get('op'), '=')
        self.assertEqual(conditions.get('metric').get('val'), "'qd'")
        self.assertEqual(limit, 3)
        self.assertEqual(order.get("orders desc").get('fld'), 'orders desc')

    def test_basic_agg_alias_select(self):
        sqm = SqlToMongo(self.schema.get("myschema"))
        select, table, conditions, group, order, limit = sqm.convert_to_mongo(
            "select user, sum(orders) as totalorders from sidtest where tradedate=20150205 and metric='qd' order by orders desc limit 3")
        print select, table, conditions, group, order, limit
        self.assertEqual(select.get("user", None).get('fld'), 'user')
        self.assertEqual(select.get("orders", None).get('alias'), 'totalorders')
        self.assertEqual(select.get("orders", None).get('type'), 'func')
        self.assertEqual(select.get("orders", None).get('op'), 'sum')
        self.assertEqual(conditions.get('metric').get('op'), '=')
        self.assertEqual(conditions.get('metric').get('val'), "'qd'")
        self.assertEqual(limit, 3)
        self.assertEqual(order.get("orders desc").get('fld'), 'orders desc')

    def test_array_agg_alias_select(self):
        sqm = SqlToMongo(self.schema.get("myschema"))
        select, table, conditions, group, order, limit = sqm.convert_to_mongo(
            "select user, sum(vals.orders) as totalorders from sidtest where tradedate=20150205 and metric='qd' order by orders desc limit 3")
        print select, table, conditions, group, order, limit
        self.assertEqual(select.get("user", None).get('fld'), 'user')
        self.assertEqual(select.get("vals.orders", None).get('fld'), 'vals.orders')
        self.assertEqual(conditions.get('metric').get('op'), '=')
        self.assertEqual(conditions.get('metric').get('val'), "'qd'")
        self.assertEqual(limit, 3)
        self.assertEqual(order.get("orders desc").get('fld'), 'orders desc')

    def test_array_agg_no_alias_select(self):
        sqm = SqlToMongo(self.schema.get("myschema"))
        select, table, conditions, group, order, limit = sqm.convert_to_mongo(
            "select user, sum(vals.orders) from sidtest where tradedate=20150205 and metric='qd' order by orders desc limit 3")
        print select, table, conditions, group, order, limit
        self.assertEqual(select.get("user", None).get('fld'), 'user')
        self.assertEqual(select.get("vals.orders", None).get('fld'), 'vals.orders')
        self.assertEqual(conditions.get('metric').get('op'), '=')
        self.assertEqual(conditions.get('metric').get('val'), "'qd'")
        self.assertEqual(limit, 3)
        self.assertEqual(order.get("orders desc").get('fld'), 'orders desc')

    def test_array_with_alias(self):
        sqm = SqlToMongo(self.schema.get("myschema"))
        select, table, conditions, group, order, limit = sqm.convert_to_mongo(
            "select userid, vals.min as valmin from sidtest where metric='latency' order by valmin desc limit 3")
        #print select, table, conditions, group, order, limit
        self.assertEqual(select.get("userid", None).get('fld'), 'userid')
        self.assertEqual(select.get("vals.min", None).get('fld'), 'vals.min')
        self.assertEqual(limit, 3)
        self.assertEqual(order.get("valmin desc").get('fld'), 'valmin desc')
        result = sqm.gen_mongo(select, table, conditions, group, order, limit)
        rows = result.get('result')
        self.assertEqual(result.get('ok'), 1.0)
        self.assertEqual(len(rows), 3)
        for i, row in enumerate(rows):
            #print row
            self.assertEqual(row.get('userid'), 'todd')
            self.assertEqual(row.get('valmin'), 12-i)



