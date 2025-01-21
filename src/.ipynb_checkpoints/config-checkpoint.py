## config.py
# Configuration file for the project

# File paths
DATA_FILE_PATH = "data/test.tsv"
OUTPUT_NODES_PATH = "output/nodes"
OUTPUT_EDGES_PATH = "output/edges"
NODES_JSON_PATH = "output/nodes.json"
EDGES_JSON_PATH = "output/edges.json"

# Spark settings
SPARK_APP_NAME = "ProteinGraphPipeline"
SPARK_EXECUTOR_MEMORY = "8g"
SPARK_DRIVER_MEMORY = "8g"
SPARK_SQL_SHUFFLE_PARTITIONS = 200

# Thresholds and parameters
SIMILARITY_THRESHOLD = 0.5
DOMAIN_SIZE_TOLERANCE = 5
PARTITION_COUNT = 100

NODES_OUTPUT_DIR = "output/nodes.json"  # Spark output directory
NODES_JSON_PATH = "output/nodes_combined.json"  # Final merged JSON file
EDGES_JSON_PATH = "output/edges_combined.json"  # Final merged JSON file


