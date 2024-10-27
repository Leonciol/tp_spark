from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, when
# from src.fr.hymaia.exo2.spark_session_provider import SparkSessionProvider


def main():
    # Créer une Spark Session
    spark = SparkSession \
        .builder \
        .appName("spark_use_case") \
        .master("local[*]") \
        .getOrCreate()

    # Lire les fichiers clients et villes
    df_clients = spark.read.option("header", True).csv("./src/resources/exo2/clients_bdd.csv")

    # Filtrer les clients majeurs (age >= 18)
    df_adult_clients = filter_major(df_clients)

    df_villes = spark.read.option("header", True).csv("./src/resources/exo2/city_zipcode.csv")

    # Jointure pour obtenir le nom de la ville
    df_joined = joined_city_df(df_adult_clients, df_villes)

    df_with_dept = add_departement(df_joined)

    # Ecrire le résultat dans un fichier parquet
    df_with_dept.write.mode("overwrite").parquet("./data/exo2/clean")

    spark.stop()
    
def filter_major(df):
        # Filtrer les clients majeurs (age >= 18)
        df_adult_clients = df.filter(df.age >= 18)
        return df_adult_clients
    
def joined_city_df(df_adult_clients,df_villes):
    df_joined = df_adult_clients.join(df_villes, on="zip")
    return df_joined

def add_departement(df):
    df_with_dept = df.withColumn(
        "departement",
        when(
            (substring(col("zip"), 1, 2) == '20') & (col("zip").cast("int") <= 20190),
            "2A"
        ).when(
            (substring(col("zip"), 1, 2) == '20') & (col("zip").cast("int") > 20190),
            "2B"
        ).otherwise(substring(col("zip"), 1, 2))
    )
    return df_with_dept



if __name__ == "__main__":
    main()
