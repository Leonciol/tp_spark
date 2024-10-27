from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

def main():
    spark = SparkSession \
        .builder \
        .appName("wordcount") \
        .master("local[*]")\
        .getOrCreate()
        
    # Lecture du fichier CSV
    df = spark.read.option("header", True).csv("./src/resources/exo1/data.csv")
    
    # Définir et appliquer la transformation wordcount
    wordcount_df = (
        # Diviser les textes en mots
        df.select(explode(split(col("text"), " ")).alias("word"))
        .groupBy("word")
        .count()
    )

    # Ecrire au format parquet, partitionné par "count"
    output_path = "data/exo1/output"
    wordcount_df.write.partitionBy("count").parquet(output_path)

if __name__ == "__main__":
    main()
