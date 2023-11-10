import unittest

from mysite.unmasque.src.util.ConnectionHelper import ConnectionHelper
from mysite.unmasque.refactored.from_clause import FromClause
from mysite.unmasque.test.util import queries


class MyTestCase(unittest.TestCase):

    def test_like_tpchq1(self):
        query = queries.tpch_query1
        conn = ConnectionHelper()
        conn.connectUsingParams()
        self.assertTrue(conn.conn is not None)
        fc = FromClause(conn)
        rels = fc.doJob(query, "error")
        self.assertEqual(len(rels), 1)
        self.assertTrue('lineitem' in rels)
        conn.closeConnection()

    def test_like_tpchq3(self):
        query = queries.tpch_query3
        conn = ConnectionHelper()
        conn.connectUsingParams()
        self.assertTrue(conn.conn is not None)
        fc = FromClause(conn)
        rels = fc.doJob(query, "rename")
        self.assertEqual(len(rels), 3)
        self.assertTrue('lineitem' in rels)
        self.assertTrue('customer' in rels)
        self.assertTrue('orders' in rels)
        conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
