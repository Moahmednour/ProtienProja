## graph_visualization.py
from pyvis.network import Network

def visualize_graph(nodes_rdd, edges_rdd, output_file="graph.html"):
    """
    Create a demo graph visualization using PyVis.

    Parameters:
        nodes_rdd (RDD): RDD of nodes.
        edges_rdd (RDD): RDD of edges.
        output_file (str): Output file for the visualization.
    """
    print("Creating graph visualization...")
    net = Network(height="750px", width="100%", directed=True, notebook=True)

    # Sample 5% of the nodes and filter edges accordingly
    sample_nodes_rdd = nodes_rdd.sample(False, 0.05, seed=42)
    sampled_node_ids = sample_nodes_rdd.map(lambda node: node["id"]).collect()
    sample_edges_rdd = edges_rdd.filter(
        lambda edge: edge["source"] in sampled_node_ids and edge["target"] in sampled_node_ids
    )

    # Add nodes
    nodes = sample_nodes_rdd.collect()
    for node in nodes:
        net.add_node(
            node["id"], 
            label=node["id"], 
            title=f"Gene: {node['gene']}\nOrganism: {node['organism']}\nDomains: {', '.join(node['domains'])}\nStatus: {node['status']}"
        )

    # Add edges
    edges = sample_edges_rdd.collect()
    for edge in edges:
        net.add_edge(
            edge["source"], 
            edge["target"], 
            value=edge["weight"], 
            title=f"Weight: {edge['weight']}"
        )

    # Configure the graph layout
    net.force_atlas_2based()

    # Save the visualization
    net.show(output_file)
    print(f"Graph visualization saved to {output_file}")

if __name__ == "__main__":
    print("This module is designed for graph visualization and should be imported into your pipeline.")
