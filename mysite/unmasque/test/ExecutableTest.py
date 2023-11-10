import unittest

from mysite.unmasque.src.util.ConnectionHelper import ConnectionHelper
from mysite.unmasque.refactored.executable import Executable


class MyTestCase(unittest.TestCase):
    def test_execute_query_on_db(self):
        query = "select count(*) from public.region;"
        conn = ConnectionHelper()
        conn.connectUsingParams()
        self.assertTrue(conn.conn is not None)

        exe = Executable(conn)
        result = exe.doJob(query)
        self.assertTrue(result is not None)
        conn.closeConnection()
        self.assertEqual(exe.method_call_count, 1)
        self.assertEqual(conn.conn, None)

        result = None
        query = "select count(*) from public.nation;"
        conn.connectUsingParams()
        self.assertTrue(conn.conn is not None)
        self.assertTrue(result is None)

        result = exe.doJob(query)
        self.assertTrue(result is not None)
        conn.closeConnection()
        self.assertEqual(conn.conn, None)
        self.assertEqual(exe.method_call_count, 2)


if __name__ == '__main__':
    unittest.main()
