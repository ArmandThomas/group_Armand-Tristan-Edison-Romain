#!/bin/bash

# Chemin de destination du fichier CSV
CSV_FILE_PATH="/shared/tp_group.csv"

# Récupérer aléatoirement une ligne du fichier CSV (en sautant la première ligne)
RANDOM_LINE=$(tail -n +2 "$CSV_FILE_PATH" | shuf -n 1)

# Envoyer la ligne au topic Kafka
echo "$RANDOM_LINE" | kafka-console-producer.sh --broker-list localhost:9092 --topic topic-tp_group
