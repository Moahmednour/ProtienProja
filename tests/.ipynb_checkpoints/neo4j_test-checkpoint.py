from neo4j import GraphDatabase


# Neo4j connection details
uri = "bolt://neo4j-container:7687"
username = "neo4j"
password = None  # No authentication since NEO4J_AUTH=none was set

driver = GraphDatabase.driver(uri, auth=(username, password))

def test_connection():
    with driver.session() as session:
        result = session.run("RETURN 'Neo4j connection successful!' AS message")
        for record in result:
            print(record["message"])

if __name__ == "__main__":
    test_connection()
    
