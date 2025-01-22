import subprocess
import sys

# Liste des packages à installer
packages = [
    'scikit-learn==1.0',
    'kafka-python==2.0.2',
    'ntplib==0.4.0',
    'numpy',
    'kafka',  
    'confluent-kafka'  
]

# Fonction pour installer un package via pip
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# Boucle pour installer chaque package dans la liste
for p in packages:
    try:
        print(f"Installation de {p}...")
        install(p)
        print(f"{p} installé avec succès!")
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors de l'installation de {p}: {e}")
