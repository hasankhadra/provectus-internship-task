import unittest
import sys

sys.path.append('/home/hasan/provectus/level1/provectus-internship-task')
from dataprocessing.main import DataProcessing

TEST_PROCESSED_DATA_PATH = "./test_output.csv"
TEST_SRC_DATA_PATH = "./test_src_data"


class TestDataProcessing(unittest.TestCase):

    def test_add_data(self):
        data_processor = DataProcessing(TEST_SRC_DATA_PATH, TEST_PROCESSED_DATA_PATH)

        # Empty the test_output.csv
        open(TEST_PROCESSED_DATA_PATH, "w+").close()

        user_data = {'user_id': '1',
                     'first_name': 'Hasan',
                     'last_name': 'Khadra',
                     'birthts': '293841924382',
                     'img_path': '/1.png'}
        data_processor.add_data(user_data)

        import csv

        read_obj = open(TEST_PROCESSED_DATA_PATH, 'r')
        csv_reader = csv.reader(read_obj)
        rows = [row for row in csv_reader]
        read_obj.close()

        self.assertEqual(rows[0], ['user_id', 'first_name', 'last_name', 'birthts', 'img_path'])
        self.assertIsInstance(int(rows[1][0]), int)
        self.assertIsInstance(int(rows[1][3]), int)

    def test_process_data(self):
        data_processor = DataProcessing(TEST_SRC_DATA_PATH, TEST_PROCESSED_DATA_PATH)

        open(TEST_PROCESSED_DATA_PATH, "w+").close()

        data_processor.process_data()

        import csv

        read_obj = open(TEST_PROCESSED_DATA_PATH, 'r')
        csv_reader = csv.reader(read_obj)
        rows = [row for row in csv_reader]
        read_obj.close()

        self.assertEqual(rows[0], ['user_id', 'first_name', 'last_name', 'birthts', 'img_path'])
        self.assertEqual(rows[1], ['1000', 'Susan', 'Lee', '612302400000', './test_src_data/1000.png'])
        self.assertEqual(rows[2], ['1001', 'Rosa', 'Garcia', '670626000000', ''])


if __name__ == "__main__":

    unittest.main()
