from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def main():
    # Créer une Spark Session
    spark = SparkSession \
        .builder \
        .appName("aggregate") \
        .master("local[*]") \
        .getOrCreate()
    
    # Lire le fichier Parquet nettoyé
    df = spark.read.parquet("./data/exo2/clean")

    # Ajouter la colonne département
    df_population = population_by_departement(df)

    # Écrire le résultat dans un fichier CSV
    df_population.write.mode("overwrite").option("header", True).csv("./data/exo2/aggregate")

    spark.stop()
    

def population_by_departement(df):
# Calculer le nombre de clients par département
    df_population_by_dept = df.groupBy("departement").agg(count("*").alias("nb_people")) \
        .orderBy(col("nb_people").desc(), col("departement").asc())
    return df_population_by_dept


if __name__ == "__main__":
    main()