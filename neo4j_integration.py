import os
from neo4j import GraphDatabase

class Neo4jIntegration:
    def __init__(self, uri="bolt://localhost:7687", username="neo4j", password=None):
        """
        Initialize the Neo4j driver.
        """
        self.driver = GraphDatabase.driver(uri, auth=(username, password))

    def close(self):
        """
        Close the Neo4j driver connection.
        """
        self.driver.close()

    def load_csv_to_neo4j(self, csv_path, query, file_type):
        """
        Generic function to load CSV data into Neo4j.

        Parameters:
            csv_path (str): Path to the CSV file.
            query (str): Cypher query for importing data.
            file_type (str): Type of data being imported (nodes/edges) for logging.
        """
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"{file_type.capitalize()} file not found: {csv_path}")

        print(f"Loading {file_type} from: {csv_path}")
        with self.driver.session() as session:
            session.run(query, file=f"file://{os.path.abspath(csv_path)}")
        print(f"{file_type.capitalize()} imported successfully.")

    def import_nodes(self, nodes_csv_path="output/node.csv"):
        """
        Import nodes from CSV into Neo4j.
        """
        query = """
        LOAD CSV WITH HEADERS FROM $file AS row
        CREATE (:Protein {
            id: row.id,
            name: row.name,
            gene: row.gene,
            organism: row.organism,
            domains: split(row.domains, '|'),
            status: row.status
        })
        """
        self.load_csv_to_neo4j(nodes_csv_path, query, "nodes")

    def import_edges(self, edges_csv_path="output/edge.csv"):
        """
        Import edges from CSV into Neo4j.
        """
        query = """
        LOAD CSV WITH HEADERS FROM $file AS row
        MATCH (source:Protein {id: row.source})
        MATCH (target:Protein {id: row.target})
        CREATE (source)-[:SIMILAR {weight: toFloat(row.weight)}]->(target)
        """
        self.load_csv_to_neo4j(edges_csv_path, query, "edges")

def import_data_to_neo4j(nodes_csv_path, edges_csv_path):
    """
    Import nodes and edges from CSV files into Neo4j.
    """
    neo4j = Neo4jIntegration()
    try:
        print("Connecting to Neo4j...")
        neo4j.import_nodes(nodes_csv_path)
        neo4j.import_edges(edges_csv_path)
    except Exception as e:
        print(f"Error during Neo4j import: {e}")
    finally:
        neo4j.close()
        print("Neo4j connection closed.")

if __name__ == "__main__":
    # Paths to the CSV files
    nodes_csv_path = "output/nodes.csv"
    edges_csv_path = "output/edges.csv"
    import_data_to_neo4j(nodes_csv_path, edges_csv_path)
