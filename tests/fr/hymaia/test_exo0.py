import unittest
from pyspark.sql import SparkSession, Row
from src.fr.hymaia.exo1.main import wordcount

class TestMain(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("unit test") \
            .master("local[*]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_wordcount(self):
        # GIVEN
        input_df = self.spark.createDataFrame(
            [
                Row(text='bonjour je suis un test unitaire'),
                Row(text='bonjour suis test')
            ]
        )
        expected_df = self.spark.createDataFrame(
            [
                Row(word='bonjour', count=2),
                Row(word='je', count=1),
                Row(word='suis', count=2),
                Row(word='un', count=1),
                Row(word='test', count=2),
                Row(word='unitaire', count=1),
            ]
        )

        actual_df = wordcount(input_df, 'text')

        self.assertCountEqual(actual_df.collect(), expected_df.collect())

if __name__ == '__main__':
    unittest.main()
