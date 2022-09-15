import pandas as pd
from neo4j import GraphDatabase

# The execution is done by Neo4j Software and Python.
#----------------------------------------------Neo4j Setting----------------------------------------
# 1. Download Neo4j Desktop(The version is 1.4.13)

# 2. Open a new project and import csv file
# connection to Neo4J database
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))
# Neo4j's default configuration enables import from local file directory, see https://neo4j.com/docs/cypher-manual/current/clauses/load-csv/
# If it is not enabled, change Neo4j'c configuration file: dbms.security.allow_csv_import_from_file_urls=true
# Neo4j's default import directory is <NEO4J_HOME>/import, 
#    to use this script
#    - EITHER change the variable path_to_neo4j_import_directory to <NEO4J_HOME>/import and move the input files to this directory
#    - OR set the import directory in Neo4j's configuration file: dbms.directories.import=
#    see https://neo4j.com/docs/cypher-manual/current/clauses/load-csv/#query-load-csv-introduction
path_to_neo4j_import_directory = 'C:\\temp\\import\\'

# 3. Install APOC library in the plug-ins to allow periodic iterations. (The version is 4.4.03)
# ensure to allocate enough memory to your database: dbms.memory.heap.max_size=20G advised
# ensure to allocate enough memory to your database: dbms.memory.heap.max_size=20G advised

# ---------------------------------Using Python to connect with Neo4j and generate event knowledge graph----------------

# Connect to Neo4j Server
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))
def run_query(driver, query: str, verbose: bool = False):
    if verbose:
        print(query, end="\n\n")
    with driver.session() as session:
        result = session.run(query).single()
        if result != None: 
            return result.value()
        else:
            return None

# Step 1: create event nodes
def create_event_query(logHeader: list, fileName: str, LogID: str = ""):
    query = f'USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM \"file:/{fileName}\" as line'
    for i, col in enumerate(logHeader):
        column = 'line.' + col
        if (i == 0 and LogID != ""):
            newLine = f' CREATE (e:Event {{Log: "{LogID}",{col}: {column},'
        elif (i == 0):
            newLine = f' CREATE (e:Event {{ {col}: {column},'
        elif (i == len(logHeader)-1):
            newLine = f' {col}: {column} }})'
        else:
            newLine = f' {col}: {column},'           
            
        query += newLine
    return query

qCleanDatabase_allRelations = f'''
CALL apoc.periodic.iterate(
"MATCH ()-[r]->() RETURN id(r) AS id",
"MATCH ()-[r]->() WHERE id(r)=id DELETE r",
{{batchSize:10000}})''' 

qCleanDatabase_allNodes = f'''
CALL apoc.periodic.iterate(
"MATCH (n) RETURN id(n) AS id",
"MATCH (n) WHERE id(n)=id DELETE n",
{{batchSize:10000}})'''

run_query(driver, qCleanDatabase_allRelations) # delete all relationships, comment out if you want to keep them
run_query(driver, qCleanDatabase_allNodes)     # delete all nodes, comment out if you want to keep them

inputFile = 'AT1.csv'
csv_log = pd.read_csv(inputFile)
header = csv_log.columns.values
qCreateEvents = create_event_query(header, "AT1.csv", 'Event')
run_query(driver, qCreateEvents)

q_countImportedEvents = "MATCH (e:Event) RETURN count(e)"
result = run_query(driver, q_countImportedEvents)
print(result)


# Step 2: create entity nodes and correlate events to entity nodes
data_entities = ['Activity', 'Project', 'Person']
for entity in data_entities:
    query_create_entity_nodes = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WITH DISTINCT e.{entity} AS id RETURN id",
        "CREATE (n:Entity {{ID:id, EntityType:'{entity}'}})",
        {{batchSize:10000}})'''
    run_query(driver, query_create_entity_nodes)

    query_correlate_events_to_entity = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE EXISTS(e.{entity}) MATCH (n:Entity {{EntityType: '{entity}'}}) WHERE e.{entity} = n.ID RETURN id(e) AS IDe, id(n) as IDn",
        "MATCH (e:Event) WHERE id(e)=IDe MATCH (n:Entity) WHERE id(n) = IDn CREATE (e)-[:CORR]->(n)",
        {{batchSize:10000}})'''
    run_query(driver, query_correlate_events_to_entity)
        
# Step 3: Create DF relationships to entity nodes based on entity type

# create DF for each entity type
for entity in data_entities:
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n:Entity) WHERE n.EntityType='{entity}'
        MATCH (n)<-[:CORR]-(e:Event)
        WITH n, e AS nodes ORDER BY e.Date
        WITH n, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n,e1,e2
        MERGE (e1)-[df:DF {{EntityType:n.EntityType}}]->(e2)",
        {{batchSize:1000}})'''
    run_query(driver, query_create_directly_follows)
        
# create DF1 for each entity type
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n:Entity) WHERE n.EntityType='{entity}'
        MATCH (n)<-[:CORR]-(e:Event)
        WITH n, e AS nodes ORDER BY e.Date
        WITH n, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n,e1,e2
        MERGE (e1)-[df:DF1 {{EntityType:n.EntityType}}]->(e2)",
        {{batchSize:1000}})'''
    run_query(driver, query_create_directly_follows)



# Optional
# Step 4: Create DF edges for events correlated to each person in each project to arrange these events on the same level
# query_create_directly_follows = f'''
#     CALL apoc.periodic.iterate(
#     "MATCH (n1:Entity) WHERE n1.ID='{'Project1'}'
#     MATCH (n1)<-[:CORR]-(e:Event)
#     MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
#     MATCH (n2)<-[:CORR]-(e:Event)
#     WITH n2, e AS nodes ORDER BY e.Date
#     WITH n2, collect(nodes) AS event_node_list
#     UNWIND range(0, size(event_node_list)-2) AS i
#     RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
#     "WITH n2,e1,e2
#     MERGE (e1)-[df :DF2 {{EntityType:'Pro1_Person'}}]->(e2)",
#     {{batchSize:1000}})'''
# run_query(driver, query_create_directly_follows) 
# query_create_directly_follows = f'''
#     CALL apoc.periodic.iterate(
#     "MATCH (n1:Entity) WHERE n1.ID='{'Project4'}'
#     MATCH (n1)<-[:CORR]-(e:Event)
#     MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
#     MATCH (n2)<-[:CORR]-(e:Event)
#     WITH n2, e AS nodes ORDER BY e.Date
#     WITH n2, collect(nodes) AS event_node_list
#     UNWIND range(0, size(event_node_list)-2) AS i
#     RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
#     "WITH n2,e1,e2
#     MERGE (e1)-[df :DF2 {{EntityType:'Pro4_Person'}}]->(e2)",
#     {{batchSize:1000}})'''
# run_query(driver, query_create_directly_follows)
# query_create_directly_follows = f'''
#     CALL apoc.periodic.iterate(
#     "MATCH (n1:Entity) WHERE n1.ID='{'Project2'}'
#     MATCH (n1)<-[:CORR]-(e:Event)
#     MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
#     MATCH (n2)<-[:CORR]-(e:Event)
#     WITH n2, e AS nodes ORDER BY e.Date
#     WITH n2, collect(nodes) AS event_node_list
#     UNWIND range(0, size(event_node_list)-2) AS i
#     RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
#     "WITH n2,e1,e2
#     MERGE (e1)-[df :DF2 {{EntityType:'Pro2_Person'}}]->(e2)",
#     {{batchSize:1000}})'''
# run_query(driver, query_create_directly_follows)
# query_create_directly_follows = f'''
#     CALL apoc.periodic.iterate(
#     "MATCH (n1:Entity) WHERE n1.ID='{'Project3'}'
#     MATCH (n1)<-[:CORR]-(e:Event)
#     MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
#     MATCH (n2)<-[:CORR]-(e:Event)
#     WITH n2, e AS nodes ORDER BY e.Date
#     WITH n2, collect(nodes) AS event_node_list
#     UNWIND range(0, size(event_node_list)-2) AS i
#     RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
#     "WITH n2,e1,e2
#     MERGE (e1)-[df :DF2 {{EntityType:'Pro3_Person'}}]->(e2)",
#     {{batchSize:1000}})'''
# run_query(driver, query_create_directly_follows)
# query_create_directly_follows = f'''
#     CALL apoc.periodic.iterate(
#     "MATCH (n1:Entity) WHERE n1.ID='{'Project5'}'
#     MATCH (n1)<-[:CORR]-(e:Event)
#     MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
#     MATCH (n2)<-[:CORR]-(e:Event)
#     WITH n2, e AS nodes ORDER BY e.Date
#     WITH n2, collect(nodes) AS event_node_list
#     UNWIND range(0, size(event_node_list)-2) AS i
#     RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
#     "WITH n2,e1,e2
#     MERGE (e1)-[df :DF2 {{EntityType:'Pro5_Person'}}]->(e2)",
#     {{batchSize:1000}})'''
# run_query(driver, query_create_directly_follows)
# query_create_directly_follows = f'''
#     CALL apoc.periodic.iterate(
#     "MATCH (n1:Entity) WHERE n1.ID='{'Project6'}'
#     MATCH (n1)<-[:CORR]-(e:Event)
#     MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
#     MATCH (n2)<-[:CORR]-(e:Event)
#     WITH n2, e AS nodes ORDER BY e.Date
#     WITH n2, collect(nodes) AS event_node_list
#     UNWIND range(0, size(event_node_list)-2) AS i
#     RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
#     "WITH n2,e1,e2
#     MERGE (e1)-[df :DF2 {{EntityType:'Pro6_Person'}}]->(e2)",
#     {{batchSize:1000}})'''
# run_query(driver, query_create_directly_follows)
# query_create_directly_follows = f'''
#     CALL apoc.periodic.iterate(
#     "MATCH (n1:Entity) WHERE n1.ID='{'Project7'}'
#     MATCH (n1)<-[:CORR]-(e:Event)
#     MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
#     MATCH (n2)<-[:CORR]-(e:Event)
#     WITH n2, e AS nodes ORDER BY e.Date
#     WITH n2, collect(nodes) AS event_node_list
#     UNWIND range(0, size(event_node_list)-2) AS i
#     RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
#     "WITH n2,e1,e2
#     MERGE (e1)-[df :DF2 {{EntityType:'Pro7_Person'}}]->(e2)",
#     {{batchSize:1000}})'''
# run_query(driver, query_create_directly_follows)
# query_create_directly_follows = f'''
#     CALL apoc.periodic.iterate(
#     "MATCH (n1:Entity) WHERE n1.ID='{'Project8'}'
#     MATCH (n1)<-[:CORR]-(e:Event)
#     MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
#     MATCH (n2)<-[:CORR]-(e:Event)
#     WITH n2, e AS nodes ORDER BY e.Date
#     WITH n2, collect(nodes) AS event_node_list
#     UNWIND range(0, size(event_node_list)-2) AS i
#     RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
#     "WITH n2,e1,e2
#     MERGE (e1)-[df :DF2 {{EntityType:'Pro8_Person'}}]->(e2)",
#     {{batchSize:1000}})'''
# run_query(driver, query_create_directly_follows)
# query_create_directly_follows = f'''
#     CALL apoc.periodic.iterate(
#     "MATCH (n1:Entity) WHERE n1.ID='{'Project9'}'
#     MATCH (n1)<-[:CORR]-(e:Event)
#     MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
#     MATCH (n2)<-[:CORR]-(e:Event)
#     WITH n2, e AS nodes ORDER BY e.Date
#     WITH n2, collect(nodes) AS event_node_list
#     UNWIND range(0, size(event_node_list)-2) AS i
#     RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
#     "WITH n2,e1,e2
#     MERGE (e1)-[df :DF2 {{EntityType:'Pro9_Person'}}]->(e2)",
#     {{batchSize:1000}})'''
# run_query(driver, query_create_directly_follows)
# query_create_directly_follows = f'''
#     CALL apoc.periodic.iterate(
#     "MATCH (n1:Entity) WHERE n1.ID='{'Project10'}'
#     MATCH (n1)<-[:CORR]-(e:Event)
#     MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
#     MATCH (n2)<-[:CORR]-(e:Event)
#     WITH n2, e AS nodes ORDER BY e.Date
#     WITH n2, collect(nodes) AS event_node_list
#     UNWIND range(0, size(event_node_list)-2) AS i
#     RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
#     "WITH n2,e1,e2
#     MERGE (e1)-[df :DF2 {{EntityType:'Pro10_Person'}}]->(e2)",
#     {{batchSize:1000}})'''
# run_query(driver, query_create_directly_follows)



