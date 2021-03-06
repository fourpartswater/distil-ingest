#!/bin/bash

DATA_DIR=~/datasets/seed_datasets_current
SCHEMA=/datasetDoc.json
OUTPUT_DATA=clusters/clusters.csv
OUTPUT_SCHEMA=clustersDatasetDoc.json
DATASET_FOLDER_SUFFIX=_dataset
DATASETS=(26_radon_seed 32_wikiqa 60_jester 185_baseball 196_autoMpg 313_spectrometer 38_sick 1491_one_hundred_plants_margin 27_wordLevels 57_hypothyroid 299_libras_move 534_cps_85_wages 1567_poker_hand 22_handgeometry)
HAS_HEADER=1
CLUSTER_FUNCTION=fileupload
REST_ENDPOINT=HTTP://localhost:5000
DATA_SERVER=HTTP://10.108.4.104

for DATASET in "${DATASETS[@]}"
do
    echo "--------------------------------------------------------------------------------"
    echo " Clustering $DATASET dataset"
    echo "--------------------------------------------------------------------------------"
    go run cmd/distil-cluster/main.go \
        --rest-endpoint="$REST_ENDPOINT" \
        --cluster-function="$CLUSTER_FUNCTION" \
        --dataset="$DATA_DIR/${DATASET}/TRAIN/dataset_TRAIN" \
        --media-path="$DATA_SERVER/${DATASET}" \
        --schema="$DATA_DIR/${DATASET}/TRAIN/dataset_TRAIN/$SCHEMA" \
        --output="$DATA_DIR/${DATASET}/TRAIN/dataset_TRAIN" \
        --output-data="$OUTPUT_DATA" \
        --output-schema="$OUTPUT_SCHEMA" \
        --has-header=$HAS_HEADER
done
