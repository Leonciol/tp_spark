import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../src')))

import unittest
from pyspark.sql import SparkSession, Row
from src.fr.hymaia.exo2.spark_clean_job import filter_major, joined_city_df, add_departement

class TestClientFunctions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession \
            .builder \
            .appName("spark_integration_test") \
            .master("local[*]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_filter_major(self):
        df_clients = self.spark.createDataFrame([
            Row(name="oui", age=15, zip="17540"),
            Row(name="non", age=18, zip="21000"),
            Row(name="ahaha", age=47, zip="21160"),
        ])
        
        df_filtered = filter_major(df_clients)
        result = [row['age'] for row in df_filtered.collect()]

        self.assertNotIn(15, result)
        self.assertIn(18, result)
        self.assertIn(47, result)

    def test_joined_city_df(self):
        df_clients = self.spark.createDataFrame([
            Row(name="non", age=18, zip="21000"),
            Row(name="ahaha", age=47, zip="21160"),
        ])

        df_villes = self.spark.createDataFrame([
            Row(zip="21000", city="Dijon"),
            Row(zip="21160", city="Marsannay-la-Côte"),
        ])

        df_joined = joined_city_df(df_clients, df_villes)
        result = df_joined.collect()

        expected = [
            ("non", 18, "21000", "Dijon"),
            ("ahaha", 47, "21160", "Marsannay-la-Côte")
        ]

        self.assertEqual(len(result), len(expected))
        for res, exp in zip(result, expected):
            self.assertEqual((res['name'], res['age'], res['zip'], res['city']), exp)
    
    def test_add_departement(self):
        df = self.spark.createDataFrame([
            Row(name="non", age=18, zip="21000"),
            Row(name="ahaha", age=47, zip="21160"),
            Row(name="corsica1", age=30, zip="20180"),
            Row(name="corsica2", age=30, zip="20250"),
        ])
        
        df_with_dept = add_departement(df)
        result = {row['name']: row['departement'] for row in df_with_dept.collect()}

        self.assertEqual(result["non"], "21")
        self.assertEqual(result["ahaha"], "21")
        self.assertEqual(result["corsica1"], "2A")
        self.assertEqual(result["corsica2"], "2B")

if __name__ == '__main__':
    unittest.main()