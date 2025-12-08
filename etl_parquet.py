import sys
from pyspark.sql import SparkSession

# --- 1. VÉRIFICATION DES ARGUMENTS ---
if len(sys.argv) < 2:
    print("Usage : spark-submit etl_parquet.py <fichier.csv>")
    sys.exit(1)

fichier_entree = sys.argv[1]

# --- 2. DÉMARRAGE SESSION ---
spark = SparkSession.builder \
    .appName("ConvertisseurParquet") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"\n--- 1. Lecture du fichier BRUT (Bronze) : {fichier_entree} ---")

# --- 3. LECTURE CSV ---
df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv(fichier_entree)

# --- 4. TRANSFORMATION (SQL) ---
df.createOrReplaceTempView("source")

# Notez que je garde 'Metier' pour pouvoir partitionner avec
df_clean = spark.sql("""
    SELECT Nom, Age, Metier 
    FROM source 
    WHERE Age < 50 
    AND Metier IS NOT NULL
""")

print("--- 2. Aperçu des données nettoyées ---")
df_clean.show()

# --- 5. ÉCRITURE PARQUET PARTITIONNÉ (Silver) ---
# On crée un nom de dossier de sortie dynamique
dossier_sortie = "output_" + fichier_entree.replace(".csv", "") + "_parquet"

print(f"--- 3. Écriture en Parquet partitionné dans : {dossier_sortie} ---")

df_clean.write \
    .mode("overwrite") \
    .partitionBy("Metier") \
    .parquet(dossier_sortie)

print("--- SUCCÈS : Conversion terminée ! ---")
spark.stop()
