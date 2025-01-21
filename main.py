## main_pipeline.py
# Main entry point for running the protein graph project
import os
import json
import shutil
from src.graph_visualization import visualize_graph
from src.utils import merge_spark_parts

from pyspark.sql import SparkSession
from src.graph_nodes import create_nodes
from src.graph_edges import create_edges
from neo4j_integration import import_data_to_neo4j
from src.config import (
    DATA_FILE_PATH,
    OUTPUT_NODES_PATH,
    OUTPUT_EDGES_PATH,
    SPARK_APP_NAME,
    SPARK_EXECUTOR_MEMORY,
    SPARK_DRIVER_MEMORY,
    SPARK_SQL_SHUFFLE_PARTITIONS,
)
from src.data_preprocessing import load_data, preprocess_data


def ensure_output_directories():
    """
    Ensure that the necessary output directories exist.
    """
    os.makedirs("output", exist_ok=True)
    os.makedirs("output/nodes", exist_ok=True)
    os.makedirs("output/edges", exist_ok=True)


def safe_save_as_textfile(rdd, path):
    """
    Save an RDD to a directory, overwriting existing files if needed.

    Parameters:
        rdd (RDD): RDD to save.
        path (str): Directory path to save the RDD.
    """
    if os.path.exists(path):
        print(f"The path {path} exists. Removing...")
        shutil.rmtree(path)
    rdd.saveAsTextFile(path)


def main():
    """
    Main function to execute the pipeline.
    """
    # Ensure output directories exist
    ensure_output_directories()

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY) \
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
        .config("spark.sql.shuffle.partitions", SPARK_SQL_SHUFFLE_PARTITIONS) \
        .getOrCreate()

    print(f"Spark session initialized with version {spark.version}")

    # Step 1: Load and preprocess data
    print("Loading and preprocessing data...")
    raw_rdd, header = load_data(spark, DATA_FILE_PATH)
    print(f"Header: {header}")
    preprocessed_rdd = preprocess_data(raw_rdd)
    print(f"Total rows after preprocessing: {preprocessed_rdd.count()}")

    # Step 2: Create nodes
    print("Creating nodes...")
    nodes_rdd = create_nodes(preprocessed_rdd, OUTPUT_NODES_PATH)

    # Verify node creation
    print(f"Total nodes created: {nodes_rdd.count()}")
    print("Sample nodes:")
    for node in nodes_rdd.take(5):
        print(node)

    # Save nodes to CSV
    print("Saving nodes to CSV...")
    nodes_csv_path = "output/nodes.csv"
    try:
        merge_spark_parts(
            input_dir=OUTPUT_NODES_PATH,
            output_file=nodes_csv_path,
            expected_fields=6,
            header=["id", "name", "gene", "organism", "domains", "status"],
        )
    except Exception as e:
        print(f"Error while saving nodes to CSV: {e}")

    # Step 3: Create edges
    print("Creating edges...")
    edges_rdd = create_edges(nodes_rdd, spark, preprocessed_rdd)

    # Verify edge creation
    print(f"Total edges created: {edges_rdd.count()}")
    print("Sample edges:")
    for edge in edges_rdd.take(5):
        print(edge)

    # Save edges to CSV
    print("Saving edges to CSV...")
    edges_csv_path = "output/edges.csv"
    try:
        merge_spark_parts(
            input_dir=OUTPUT_EDGES_PATH,
            output_file=edges_csv_path,
            expected_fields=3,
            header=["source", "target", "weight"],
        )
    except Exception as e:
        print(f"Error while saving edges to CSV: {e}")

    # Step 4: Import data into Neo4j
    print("Importing data into Neo4j...")
    try:
        import_data_to_neo4j(nodes_csv_path, edges_csv_path)
    except Exception as e:
        print(f"Error during Neo4j import: {e}")

    # Step 5: Visualize the graph
    print("Visualizing the graph...")
    visualize_graph(nodes_rdd, edges_rdd, output_file="graph.html")

    print("Graph construction, import into Neo4j, and visualization completed successfully.")


if __name__ == "__main__":
    main()
