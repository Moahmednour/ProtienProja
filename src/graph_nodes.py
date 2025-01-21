## graph_nodes.py
# Module for constructing nodes for the protein graph

import os
import shutil
import json
import csv

def create_node(row):
    """
    Create a single node dictionary from a row of data.

    Parameters:
        row (dict): Row data from the RDD.

    Returns:
        dict or None: Node dictionary or None if invalid.
    """
    if not row.get("Entry") or not row.get("Sequence"):
        return None
    return {
        "id": row["Entry"],
        "name": row.get("Protein names", ""),
        "gene": row.get("Gene Names", ""),
        "organism": row.get("Organism", ""),
        "domains": row.get("InterPro", []),
        "status": "Known" if row.get("EC number") else "Unknown"
    }

def create_nodes(preprocessed_rdd, output_nodes_path, output_file="output/node.csv"):
    """
    Create nodes from the preprocessed RDD and save them to the specified path.

    Parameters:
        preprocessed_rdd (RDD): The preprocessed RDD.
        output_nodes_path (str): Path to save the nodes JSON data.
        output_file (str): Path to save the nodes CSV file.

    Returns:
        RDD: The RDD of nodes.
    """
    # Create nodes RDD by mapping and filtering invalid rows
    nodes_rdd = preprocessed_rdd.map(create_node).filter(lambda x: x is not None)
    
    # Save the nodes to JSON format
    safe_save_as_textfile(nodes_rdd, output_nodes_path)
    
    # Save nodes to CSV format
    try:
        save_to_file = not os.path.exists(output_file)
        with open(output_file, "a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            
            # Write the header if the file is being created for the first time
            if save_to_file:
                writer.writerow(["id", "name", "gene", "organism", "domains", "status"])
            
            # Write nodes to the CSV file
            for node in nodes_rdd.collect():
                writer.writerow([
                    node["id"],
                    node["name"],
                    node["gene"],
                    node["organism"],
                    "|".join(node["domains"]),
                    node["status"]
                ])
    except Exception as e:
        print(f"Error while saving nodes to CSV: {e}")
    
    # Display sample nodes and total count
    print("Sample nodes:")
    for node in nodes_rdd.take(5):
        print(node)
    print(f"Total nodes: {nodes_rdd.count()}")
    
    return nodes_rdd

def safe_save_as_textfile(rdd, path):
    """
    Save an RDD to a directory, overwriting existing files if needed.

    Parameters:
        rdd (RDD): RDD to save.
        path (str): Directory path for saving the RDD.
    """
    if os.path.exists(path):
        print(f"The path {path} exists. Removing...")
        shutil.rmtree(path)
    rdd.map(lambda x: json.dumps(x)).saveAsTextFile(path)
    print(f"Files saved to: {path}")

def save_nodes_to_csv(nodes_rdd, output_path):
    """
    Save nodes RDD as a single CSV file.

    Parameters:
        nodes_rdd (RDD): RDD of nodes.
        output_path (str): Path to save the CSV file.
    """
    nodes_rdd = nodes_rdd.map(
        lambda node: f'{node["id"]},{node["name"]},{node["gene"]},{node["organism"]},{"|".join(node["domains"])},{node["status"]}'
    )

    temp_path = output_path + "_temp"
    nodes_rdd.saveAsTextFile(temp_path)

    with open(output_path, "w", encoding="utf-8") as output_file:
        output_file.write("id,name,gene,organism,domains,status\n")  # Add header
        for part_file in sorted(os.listdir(temp_path)):
            with open(os.path.join(temp_path, part_file), "r", encoding="utf-8") as part:
                output_file.write(part.read())
    shutil.rmtree(temp_path)

 
