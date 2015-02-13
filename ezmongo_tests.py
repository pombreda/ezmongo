import unittest
from ezmongo_main import SqlToMongo

class EZMongoTests(unittest.TestCase):
    def setUp(self):
        print "setUP"
        self.schema = {}
        execfile("testconf.py", self.schema)


    def tearDown(self):
        print "TearDown"
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