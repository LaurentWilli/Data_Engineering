import sys
from random import random
from operator import add
from pyspark.sql import SparkSession

# On initialise Spark
spark = SparkSession.builder.appName("StressTestPro").getOrCreate()

# On définit le nombre de "tirs" (plus c'est haut, plus ça chauffe)
# 100 millions de calculs
n = 100000000 
partitions = 100 # On découpe le travail en 100 petits morceaux

def f(_):
    x = random() * 2 - 1
    y = random() * 2 - 1
    return 1 if x ** 2 + y ** 2 <= 1 else 0

print(f"--- Démarrage du calcul intensif sur {n} points ---")

# Le calcul distribué
count = spark.sparkContext.parallelize(range(1, n + 1), partitions) \
    .map(f).reduce(add)

print(f"Pi est environ égal à {4.0 * count / n}")

spark.stop()
