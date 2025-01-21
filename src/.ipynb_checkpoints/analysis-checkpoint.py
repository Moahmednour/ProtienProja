## analysis.py
# Module for analyzing and visualizing the protein graph

import matplotlib.pyplot as plt

def visualize_sequence_length_distribution(classified_rdd):
    """
    Visualize the distribution of protein sequence lengths.

    Parameters:
        classified_rdd (RDD): Classified data RDD.
    """
    lengths = classified_rdd.map(lambda row: len(row["Sequence"])).collect()
    plt.figure(figsize=(10, 6))
    plt.hist(lengths, bins=50, color='blue', alpha=0.7, edgecolor='black')
    plt.title("Protein Sequence Length Distribution", fontsize=16)
    plt.xlabel("Sequence Length", fontsize=14)
    plt.ylabel("Frequency", fontsize=14)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.show()

def compute_node_degrees(edges_rdd):
    """
    Compute and display the degree of each node.

    Parameters:
        edges_rdd (RDD): RDD of edges.

    Returns:
        RDD: Node degrees.
    """
    node_degrees = edges_rdd.flatMap(lambda edge: [(edge["source"], 1), (edge["target"], 1)]) \
                              .reduceByKey(lambda a, b: a + b)
    top_degrees = node_degrees.takeOrdered(10, key=lambda x: -x[1])

    print("Top 10 nodes by degree:")
    for node, degree in top_degrees:
        print(f"Node {node} has degree {degree}")

    # Visualization of node degrees (histogram)
    degrees = node_degrees.map(lambda x: x[1]).collect()
    plt.figure(figsize=(10, 6))
    plt.hist(degrees, bins=50, color='green', alpha=0.7, edgecolor='black')
    plt.title("Node Degree Distribution", fontsize=16)
    plt.xlabel("Degree", fontsize=14)
    plt.ylabel("Frequency", fontsize=14)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.show()

    return node_degrees

def display_graph_summary(nodes_rdd, edges_rdd):
    """
    Display a summary of the graph, including node and edge counts.

    Parameters:
        nodes_rdd (RDD): RDD of nodes.
        edges_rdd (RDD): RDD of edges.
    """
    total_nodes = nodes_rdd.count()
    total_edges = edges_rdd.count()

    print(f"Total nodes: {total_nodes}")
    print(f"Total edges: {total_edges}")

def analyze_proteins_per_organism(nodes_rdd):
    """
    Analyze and visualize the distribution of proteins per organism.

    Parameters:
        nodes_rdd (RDD): RDD of nodes.
    """
    proteins_per_organism = nodes_rdd.map(lambda node: (node["organism"], 1)) \
                                   .reduceByKey(lambda a, b: a + b) \
                                   .collect()

    organisms, counts = zip(*sorted(proteins_per_organism, key=lambda x: -x[1]))

    print("Proteins per organism:")
    for organism, count in proteins_per_organism[:10]:
        print(f"{organism}: {count}")

    # Visualization
    plt.figure(figsize=(12, 8))
    plt.bar(organisms[:10], counts[:10], color='purple', alpha=0.7, edgecolor='black')
    plt.title("Top 10 Organisms by Protein Count", fontsize=16)
    plt.xlabel("Organism", fontsize=14)
    plt.ylabel("Protein Count", fontsize=14)
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()

def compute_edge_weight_statistics(edges_rdd):
    """
    Compute and display statistics of edge weights (similarity scores).

    Parameters:
        edges_rdd (RDD): RDD of edges.
    """
    weights = edges_rdd.map(lambda edge: edge["weight"]).collect()

    if weights:
        min_weight = min(weights)
        max_weight = max(weights)
        avg_weight = sum(weights) / len(weights)

        print(f"Edge Weight Statistics:\n  Min: {min_weight:.4f}\n  Max: {max_weight:.4f}\n  Avg: {avg_weight:.4f}")

        # Visualization
        plt.figure(figsize=(10, 6))
        plt.hist(weights, bins=50, color='orange', alpha=0.7, edgecolor='black')
        plt.title("Edge Weight Distribution", fontsize=16)
        plt.xlabel("Weight (Similarity)", fontsize=14)
        plt.ylabel("Frequency", fontsize=14)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.show()
    else:
        print("No weights found in edges.")
