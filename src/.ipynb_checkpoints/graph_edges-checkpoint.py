## graph_edges.py
# Module for constructing edges for the protein graph

import os
import shutil
import json
import csv

def jaccard_similarity(domains1, domains2):
    """
    Calculate the Jaccard similarity between two domain sets.

    Parameters:
        domains1 (list): List of domains for the first node.
        domains2 (list): List of domains for the second node.

    Returns:
        float: Jaccard similarity score.
    """
    set1, set2 = set(domains1), set(domains2)
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    return intersection / union if union > 0 else 0


def should_compare(node1, node2):
    """
    Check if two nodes should be compared based on domain size.

    Parameters:
        node1 (dict): First node.
        node2 (dict): Second node.

    Returns:
        bool: True if nodes should be compared, False otherwise.
    """
    len1, len2 = len(node1["domains"]), len(node2["domains"])
    return abs(len1 - len2) <= 5


def hash_domains(domains):
    """
    Generate a hash for domains to group similar nodes.

    Parameters:
        domains (list): List of domains.

    Returns:
        int: Hash value.
    """
    return hash(tuple(sorted(domains))) % 100


def create_partitioned_edges(node, partitioned_data, output_file="output/edge.csv"):
    """
    Generate edges for a node by comparing it with nodes in the opposite category
    (unknown nodes are compared to known nodes only). Saves all edges to a file
    and returns a preview of the first 5 edges.

    Parameters:
        node (dict): Source node.
        partitioned_data (dict): Partitioned nodes.
        output_file (str): File to save all edges.

    Returns:
        list: Preview of edges (first 5).
    """
    source = node["id"]
    source_domains = node["domains"]
    source_status = node["status"]
    source_hash = hash_domains(source_domains)
    all_edges = []

    # Only proceed if the source is an "Unknown" node
    if source_status != "Unknown":
        return []

    # Open file for saving edges
    save_to_file = not os.path.exists(output_file)
    with open(output_file, "a", newline="") as file:
        writer = csv.writer(file)
        if save_to_file:
            # Write CSV header if file is being created for the first time
            writer.writerow(["Source", "Target", "Weight"])

        for target_node in partitioned_data.get(source_hash, []):
            target = target_node["id"]
            target_domains = target_node["domains"]
            target_status = target_node["status"]

            # Only compare if the target is "Known"
            if target_status != "Known":
                continue

            similarity = jaccard_similarity(source_domains, target_domains)
            if similarity > 0.7:  
                edge = {"source": source, "target": target, "weight": similarity}
                all_edges.append(edge)
                writer.writerow([source, target, similarity])  # Save to file

    # Return only the first 5 edges for preview
    return all_edges[:5]


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
    rdd.map(lambda x: json.dumps(x)).saveAsTextFile(path)
    print(f"Files saved to: {path}")

def save_edges_to_csv(edges_rdd, output_path):
    """
    Save edges RDD as a single CSV file.

    Parameters:
        edges_rdd (RDD): RDD of edges.
        output_path (str): Path to save the CSV file.
    """
    # Convert RDD to CSV format
    edges_rdd = edges_rdd.map(lambda edge: f'{edge["source"]},{edge["target"]},{edge["weight"]}')

    # Save as a single file
    temp_path = output_path + "_temp"
    edges_rdd.saveAsTextFile(temp_path)

    # Merge part files into a single CSV
    with open(output_path, "w") as output_file:
        output_file.write("source,target,weight\n")  # Add header
        for part_file in sorted(os.listdir(temp_path)):
            with open(os.path.join(temp_path, part_file), "r") as part:
                output_file.write(part.read())

    # Remove the temporary directory
    shutil.rmtree(temp_path)

def create_edges(nodes_rdd, spark, preprocessed_rdd):
    """
    Create edges for the graph by comparing unknown nodes with known nodes.

    Parameters:
        nodes_rdd (RDD): Nodes RDD.
        spark (SparkSession): Spark session for processing.
        preprocessed_rdd (RDD): Preprocessed RDD.

    Returns:
        RDD: The RDD of created edges.
    """
    # Debugging: Check node status distribution
    status_counts = nodes_rdd.map(lambda node: (node["status"], 1)).reduceByKey(lambda a, b: a + b).collect()
    print("Node status distribution:", status_counts)

    print("Hashing and partitioning nodes...")
    nodes_with_hashes = nodes_rdd.map(lambda node: {**node, "hash": hash_domains(node["domains"])})
    partitioned_data = nodes_with_hashes.groupBy(lambda node: node["hash"]).mapValues(list).collectAsMap()
    partitioned_data_broadcast = spark.sparkContext.broadcast(partitioned_data)

    print("Generating edges (Unknown -> Known)...")
    edges_rdd = nodes_rdd.flatMap(lambda node: create_partitioned_edges(node, partitioned_data_broadcast.value))

    print("Saving edges...")
    safe_save_as_textfile(edges_rdd, 'output/edges')

    print("Sample edges:")
    sample_edges = edges_rdd.take(5)
    if sample_edges:
        for edge in sample_edges:
            print(edge)
    else:
        print("No edges created.")

    print(f"Total edges: {edges_rdd.count()}")

    return edges_rdd  # Ensure edges_rdd is returned
