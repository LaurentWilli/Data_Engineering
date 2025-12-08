import sys  # <--- 1. On importe le module système
from pyspark.sql import SparkSession

# 2. Sécurité : On vérifie si l'utilisateur a bien donné un fichier
if len(sys.argv) < 2:
    print("ERREUR : Vous devez spécifier un fichier à traiter !")
    print("Usage : spark-submit mon_script.py <nom_du_fichier.csv>")
    sys.exit(1) # On arrête tout immédiatement

# 3. On récupère le nom du fichier
fichier_entree = sys.argv[1]

spark = SparkSession.builder \
    .appName("ScriptDynamique") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"--- Traitement du fichier : {fichier_entree} ---")

# 4. On utilise la variable au lieu du nom en dur
df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv(fichier_entree) # <--- Ici !

# ... Le reste du script (SQL, show) est identique ...
df.createOrReplaceTempView("data")
df_resultat = spark.sql("SELECT Nom, Age FROM data WHERE Age < 40")

print("Résultats trouvés :")
df_resultat.show()

# Pour éviter d'écraser toujours le même dossier, on peut aussi dynamiser la sortie
dossier_sortie = "resultat_" + fichier_entree.replace(".csv", "")
df_resultat.write.mode("overwrite").option("header", "true").csv(dossier_sortie)
print(f"--- Sauvegardé dans {dossier_sortie} ---")

spark.stop()
