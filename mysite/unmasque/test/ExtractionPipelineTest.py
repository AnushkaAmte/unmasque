import unittest

from mysite.unmasque.src.core import ExtractionPipeLine
from mysite.unmasque.test.util import queries


class MyTestCase(unittest.TestCase):

    def test_for_tpch_query1(self):
        q_key = 'tpch_query1'
        query = queries.queries_dict[q_key]
        eq, _ = ExtractionPipeLine.extract(query)
        self.assertTrue(eq is not None)
        print(eq)

    def test_all_sample_queries(self):
        Q_keys = queries.queries_dict.keys()
        f = open("experiment_results.txt", "w")
        q_no = 1
        for q_key in Q_keys:
            query = queries.queries_dict[q_key]
            f.write("\n" + str(q_no) + ":")
            f.write("\tHidden Query:\n")
            f.write(query)
            eq, _ = ExtractionPipeLine.extract(query)
            f.write("\n*** Extracted Query:\n")
            self.assertTrue(eq is not None)
            if eq is None:
                f.write("Extraction Failed!")
            else:
                f.write(eq)
            f.write("\n---------------------------------------\n")
            q_no += 1

        f.close()


if __name__ == '__main__':
    unittest.main()
