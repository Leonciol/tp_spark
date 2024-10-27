import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../src')))

import unittest
from pyspark.sql import SparkSession, Row
from src.fr.hymaia.exo2.spark_clean_job import filter_major, joined_city_df, add_departement

class TestIntegrationSparkJob(unittest.TestCase):
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

    def test_integration_pipeline(self):
        # Simuler les données de clients
        df_clients = self.spark.createDataFrame([
            Row(name="oui", age=15, zip="17540"),
            Row(name="non", age=18, zip="21000"),
            Row(name="ahaha", age=47, zip="20160"),
        ])
        
        # Simuler les données de villes
        df_villes = self.spark.createDataFrame([
            Row(zip="21000", city="Dijon"),
            Row(zip="20160", city="Ajaccio")
        ])

        # Exécuter le pipeline complet
        df_adult_clients = filter_major(df_clients)
        df_joined = joined_city_df(df_adult_clients, df_villes)
        df_with_dept = add_departement(df_joined)

        # Collecter et vérifier les résultats
        result = {row['name']: row['departement'] for row in df_with_dept.collect()}

        self.assertEqual(result.get("non"), "21")
        self.assertEqual(result.get("ahaha"), "2A")
        self.assertNotIn("oui", result)  # Vérifier que "oui" a été filtré

if __name__ == '__main__':
    unittest.main()
