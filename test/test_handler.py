import sys
sys.path.append("..")

import unittest
from ..db_value import DBValue
from ..aggregator import AggregatorFactory
from ..bucket_timeline_handler import BucketTimelineHandler


class TestHandlerMethods(unittest.TestCase):
    def setUp(self):
        self.db_result = """
1506376740|60|lib.atoms.get|11.0|counter

1506376800|60|lib.atoms.get|12.0|counter

1506377520|60|lib.atoms.get|11.0|counter

1506377580|60|lib.atoms.get|1.0|counter

1506377880|60|lib.atoms.get|24.0|counter

1506378660|60|lib.atoms.get|11.0|counter

1506378720|60|lib.atoms.get|1.0|counter
        """
        db_values = list(map(DBValue.from_db_string, self.db_result.split("\n")[1::2]))

        self.bucket_handler = BucketTimelineHandler(bucket_name=db_values[0].bucket_name,
                                       bucket_type=db_values[0].bucket_type,
                                       db_values=db_values,
                                       aggregator=AggregatorFactory.get_aggregator(db_values[0].bucket_type))

    def test_bucket_handler_stat(self):
        result = self.bucket_handler.stat(bucket_size=600, start=1506376200, end=1506386200)
        good_result = [11.0, 12.0, 36.0, 0, 12.0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        self.assertEqual(len(result), len(good_result), "incorrect output length. should be {}, got {}"\
                         .format(len(good_result), len(result)))
        self.assertEqual(all([r == gr for (r, gr) in zip(result, good_result)]),
                         True,
                         "Expected: {}\n Got: {}".format(good_result, result))

