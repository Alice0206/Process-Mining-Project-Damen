# The execution is done by Neo4j Software and Python.
#----------------------------------------------Neo4j Setting----------------------------------------
# 1. Download Neo4j Desktop(The version is 4.4.03)

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

# 3. Install APOC library in the plug-ins to allow periodic iterations.
# ensure to allocate enough memory to your database: dbms.memory.heap.max_size=20G advised
% # ensure to allocate enough memory to your database: dbms.memory.heap.max_size=20G advised

# ---------------------------------Using Python to connect with Neo4j and generate event knowledge graph----------------
import pandas as pd
import time, os, csv
from neo4j import GraphDatabase

# connect to Neo4j Server
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))
def runQuery(driver, query):
    print('\n'+query)
    with driver.session() as session:
        result = session.run(query).single()
        if result != None: 
            return result.value()
        else:
            return None

# read event data to Neo4j
inputFile = 'AT1.csv' 
def LoadLog(localFile):
    datasetList = []
    headerCSV = []
    i = 0
    with open(localFile) as f:
        reader = csv.reader(f)
        for row in reader:
            if (i==0):
                headerCSV = list(row)
                i +=1
            else:
                datasetList.append(row)
    log = pd.DataFrame(datasetList,columns=headerCSV)
    log['Date'] = pd.to_datetime(log['Date'], format='%Y-%m-%d')
    return headerCSV, log

# Step 1: create event nodes
def CreateEventQuery(logHeader, fileName, LogID = ""):
    query = f'USING PERIODIC COMMIT LOAD CSV WITH HEADERS
    FROM \"file:/{fileName}\" as line'
    for col in logHeader:
        if col in ['Date']:
             column = f'line.{col}'
        else:
            column = 'line.'+col
        newLine = ''
        if (logHeader.index(col) == 0 and LogID != ""):
            newLine = f' CREATE (e:Event {{Log: "{LogID}",{col}: {column},'
        elif (logHeader.index(col) == 0):
            newLine = f' CREATE (e:Event {{ {col}: {column},'
        else:
            newLine = f' {col}: {column},'
        if (logHeader.index(col) == len(logHeader)-1):
            newLine = f' {col}: {column} }})'
            
        query = query + newLine
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

runQuery(driver, qCleanDatabase_allRelations) # delete all relationships, comment out if you want to keep them
runQuery(driver, qCleanDatabase_allNodes)     # delete all nodes, comment out if you want to keep them

 
header, csvLog = LoadLog(inputFile)
qCreateEvents = CreateEventQuery(header, "AT1.csv", 'Event')
qCreateEvents
runQuery(driver, qCreateEvents)

q_countImportedEvents = "MATCH (e:Event) RETURN count(e)"
result = runQuery(driver, q_countImportedEvents) 
print (result)


# Step 2: create entity nodes and correlate events to entity nodes


data_entities = ['Activity','Project','Person']


for entity in data_entities:
    query_create_entity_nodes = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WITH DISTINCT e.{entity} AS id RETURN id",
        "CREATE (n:Entity {{ID:id, EntityType:'{entity}'}})",
        {{batchSize:10000}})'''
    runQuery(driver, query_create_entity_nodes)

    query_correlate_events_to_entity = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE EXISTS(e.{entity}) MATCH (n:Entity {{EntityType: '{entity}'}}) WHERE e.{entity} = n.ID RETURN id(e) AS IDe, id(n) as IDn",
        "MATCH (e:Event) WHERE id(e)=IDe MATCH (n:Entity) WHERE id(n) = IDn CREATE (e)-[:CORR]->(n)",
        {{batchSize:10000}})'''
    runQuery(driver, query_correlate_events_to_entity)
        
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
    runQuery(driver, query_create_directly_follows)
        
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
  runQuery(driver, query_create_directly_follows)

# Step 4: Create DF relationships to entity nodes based on entity ID

# create DF for each entity ID

  query_create_directly_follows = f'''
      CALL apoc.periodic.iterate(
      "MATCH (e:Event) WHERE e.Value = '{'1'}'
      MATCH (n1:Entity) WHERE n1.ID='{'Project4'}'
      MATCH (n1)<-[:CORR]-(e:Event)
      WITH n1, e AS nodes ORDER BY e.Date
      WITH n1, collect(nodes) AS event_node_list
      UNWIND range(0, size(event_node_list)-2) AS i
      RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
      "WITH n1,e1,e2
      MERGE (e1)-[df :DF{{EntityType:'AT_Pro4'}}]->(e2)",
      {{batchSize:1000}})'''
  runQuery(driver, query_create_directly_follows)  
  query_create_directly_follows = f'''
          CALL apoc.periodic.iterate(
          "MATCH (e:Event) WHERE e.Value = '{'1'}'
          MATCH (n1:Entity) WHERE n1.ID='{'Project1'}'
          MATCH (n1)<-[:CORR]-(e:Event)
          WITH n1, e AS nodes ORDER BY e.Date
          WITH n1, collect(nodes) AS event_node_list
          UNWIND range(0, size(event_node_list)-2) AS i
          RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
          "WITH n1,e1,e2
          MERGE (e1)-[df :DF{{EntityType:'AT_Pro1'}}]->(e2)",
          {{batchSize:1000}})'''
  runQuery(driver, query_create_directly_follows)
  query_create_directly_follows = f'''
          CALL apoc.periodic.iterate(
          "MATCH (e:Event) WHERE e.Value = '{'1'}'
          MATCH (n1:Entity) WHERE n1.ID='{'Project2'}'
          MATCH (n1)<-[:CORR]-(e:Event)
          WITH n1, e AS nodes ORDER BY e.Date
          WITH n1, collect(nodes) AS event_node_list
          UNWIND range(0, size(event_node_list)-2) AS i
          RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
          "WITH n1,e1,e2
          MERGE (e1)-[df :DF{{EntityType:'AT_Pro2'}}]->(e2)",
          {{batchSize:1000}})'''
  runQuery(driver, query_create_directly_follows)
  query_create_directly_follows = f'''
          CALL apoc.periodic.iterate(
          "MATCH (e:Event) WHERE e.Value = '{'1'}'
          MATCH (n1:Entity) WHERE n1.ID='{'Project3'}'
          MATCH (n1)<-[:CORR]-(e:Event)
          WITH n1, e AS nodes ORDER BY e.Date
          WITH n1, collect(nodes) AS event_node_list
          UNWIND range(0, size(event_node_list)-2) AS i
          RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
          "WITH n1,e1,e2
          MERGE (e1)-[df :DF {{EntityType:'AT_Pro3'}}]->(e2)",
          {{batchSize:1000}})'''
  runQuery(driver, query_create_directly_follows) 
  query_create_directly_follows = f'''
          CALL apoc.periodic.iterate(
          "MATCH (e:Event) WHERE e.Value = '{'1'}'
          MATCH (n1:Entity) WHERE n1.ID='{'Project5'}'
          MATCH (n1)<-[:CORR]-(e:Event)
          WITH n1, e AS nodes ORDER BY e.Date
          WITH n1, collect(nodes) AS event_node_list
          UNWIND range(0, size(event_node_list)-2) AS i
          RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
          "WITH n1,e1,e2
          MERGE (e1)-[df :DF {{EntityType:'AT_Pro5'}}]->(e2)",
          {{batchSize:1000}})'''
  runQuery(driver, query_create_directly_follows)    
  query_create_directly_follows = f'''
          CALL apoc.periodic.iterate(
          "MATCH (e:Event) WHERE e.Value = '{'1'}'
          MATCH (n1:Entity) WHERE n1.ID='{'Project6'}'
          MATCH (n1)<-[:CORR]-(e:Event)
          WITH n1, e AS nodes ORDER BY e.Date
          WITH n1, collect(nodes) AS event_node_list
          UNWIND range(0, size(event_node_list)-2) AS i
          RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
          "WITH n1,e1,e2
          MERGE (e1)-[df:DF{{EntityType:'AT_Pro6'}}]->(e2)",
          {{batchSize:1000}})'''
  runQuery(driver, query_create_directly_follows)
  query_create_directly_follows = f'''
          CALL apoc.periodic.iterate(
          "MATCH (e:Event) WHERE e.Value = '{'1'}'
          MATCH (n1:Entity) WHERE n1.ID='{'Project7'}'
          MATCH (n1)<-[:CORR]-(e:Event)
          WITH n1, e AS nodes ORDER BY e.Date
          WITH n1, collect(nodes) AS event_node_list
          UNWIND range(0, size(event_node_list)-2) AS i
          RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
          "WITH n1,e1,e2
          MERGE (e1)-[df :DF {{EntityType:'AT_Pro7'}}]->(e2)",
          {{batchSize:1000}})'''
  runQuery(driver, query_create_directly_follows)
  query_create_directly_follows = f'''
          CALL apoc.periodic.iterate(
          "MATCH (e:Event) WHERE e.Value = '{'1'}'
          MATCH (n1:Entity) WHERE n1.ID='{'Project8'}'
          MATCH (n1)<-[:CORR]-(e:Event)
          WITH n1, e AS nodes ORDER BY e.Date
          WITH n1, collect(nodes) AS event_node_list
          UNWIND range(0, size(event_node_list)-2) AS i
          RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
          "WITH n1,e1,e2
          MERGE (e1)-[df:DF{{EntityType:'AT_Pro8'}}]->(e2)",
          {{batchSize:1000}})'''
  runQuery(driver, query_create_directly_follows)
  query_create_directly_follows = f'''
          CALL apoc.periodic.iterate(
          "MATCH (e:Event) WHERE e.Value = '{'1'}'
          MATCH (n1:Entity) WHERE n1.ID='{'Project9'}'
          MATCH (n1)<-[:CORR]-(e:Event)
          WITH n1, e AS nodes ORDER BY e.Date
          WITH n1, collect(nodes) AS event_node_list
          UNWIND range(0, size(event_node_list)-2) AS i
          RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
          "WITH n1,e1,e2
          MERGE (e1)-[df:DF {{EntityType:'AT_Pro9'}}]->(e2)",
          {{batchSize:1000}})'''
  runQuery(driver, query_create_directly_follows)
  query_create_directly_follows = f'''
          CALL apoc.periodic.iterate(
          "MATCH (e:Event) WHERE e.Value = '{'1'}'
          MATCH (n1:Entity) WHERE n1.ID='{'Project10'}'
          MATCH (n1)<-[:CORR]-(e:Event)
          WITH n1, e AS nodes ORDER BY e.Date
          WITH n1, collect(nodes) AS event_node_list
          UNWIND range(0, size(event_node_list)-2) AS i
          RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
          "WITH n1,e1,e2
          MERGE (e1)-[df:DF {{EntityType:'AT_Pro10'}}]->(e2)",
          {{batchSize:1000}})'''
  runQuery(driver, query_create_directly_follows)
  
  
  



# create DF1 for each entity ID
if periodic_commit:   
  query_create_directly_follows = f'''
      CALL apoc.periodic.iterate(
      "MATCH (n1:Entity) WHERE n1.ID='{'Project4'}'
      MATCH (n1)<-[:CORR]-(e:Event)
      WITH n1, e AS nodes ORDER BY e.Date
      WITH n1, collect(nodes) AS event_node_list
      UNWIND range(0, size(event_node_list)-2) AS i
      RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
      "WITH n1,e1,e2
      MERGE (e1)-[df :DF1 {{EntityType:'AT_Pro4'}}]->(e2)",
      {{batchSize:1000}})'''
  runQuery(driver, query_create_directly_follows)  
  query_create_directly_follows = f'''
          CALL apoc.periodic.iterate(
          "MATCH (n1:Entity) WHERE n1.ID='{'Project1'}'
          MATCH (n1)<-[:CORR]-(e:Event)
          WITH n1, e AS nodes ORDER BY e.Date
          WITH n1, collect(nodes) AS event_node_list
          UNWIND range(0, size(event_node_list)-2) AS i
          RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
          "WITH n1,e1,e2
          MERGE (e1)-[df1 :DF1 {{EntityType:'AT_Pro1'}}]->(e2)",
          {{batchSize:1000}})'''
  runQuery(driver, query_create_directly_follows)
  query_create_directly_follows = f'''
          CALL apoc.periodic.iterate(
          "MATCH (n1:Entity) WHERE n1.ID='{'Project2'}'
          MATCH (n1)<-[:CORR]-(e:Event)
          WITH n1, e AS nodes ORDER BY e.Date
          WITH n1, collect(nodes) AS event_node_list
          UNWIND range(0, size(event_node_list)-2) AS i
          RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
          "WITH n1,e1,e2
          MERGE (e1)-[df :DF1 {{EntityType:'AT_Pro2'}}]->(e2)",
          {{batchSize:1000}})'''
  runQuery(driver, query_create_directly_follows)
  query_create_directly_follows = f'''
          CALL apoc.periodic.iterate(
          "MATCH (n1:Entity) WHERE n1.ID='{'Project3'}'
          MATCH (n1)<-[:CORR]-(e:Event)
          WITH n1, e AS nodes ORDER BY e.Date
          WITH n1, collect(nodes) AS event_node_list
          UNWIND range(0, size(event_node_list)-2) AS i
          RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
          "WITH n1,e1,e2
          MERGE (e1)-[df :DF1 {{EntityType:'AT_Pro3'}}]->(e2)",
          {{batchSize:1000}})'''
  runQuery(driver, query_create_directly_follows) 
  query_create_directly_follows = f'''
          CALL apoc.periodic.iterate(
          "MATCH (n)<-[:CORR]-(e:Event)
          MATCH (n1:Entity) WHERE n1.ID='{'Project5'}'
          MATCH (n1)<-[:CORR]-(e:Event)
          WITH n1, e AS nodes ORDER BY e.Date
          WITH n1, collect(nodes) AS event_node_list
          UNWIND range(0, size(event_node_list)-2) AS i
          RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
          "WITH n1,e1,e2
          MERGE (e1)-[df :DF1 {{EntityType:'AT_Pro5'}}]->(e2)",
          {{batchSize:1000}})'''
  runQuery(driver, query_create_directly_follows)    
  query_create_directly_follows = f'''
          CALL apoc.periodic.iterate(
          "MATCH (n1:Entity) WHERE n1.ID='{'Project6'}'
          MATCH (n1)<-[:CORR]-(e:Event)
          WITH n1, e AS nodes ORDER BY e.Date
          WITH n1, collect(nodes) AS event_node_list
          UNWIND range(0, size(event_node_list)-2) AS i
          RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
          "WITH n1,e1,e2
          MERGE (e1)-[df :DF1 {{EntityType:'AT_Pro6'}}]->(e2)",
          {{batchSize:1000}})'''
  runQuery(driver, query_create_directly_follows)
  query_create_directly_follows = f'''
          CALL apoc.periodic.iterate(
          "MATCH (n1:Entity) WHERE n1.ID='{'Project7'}'
          MATCH (n1)<-[:CORR]-(e:Event)
          WITH n1, e AS nodes ORDER BY e.Date
          WITH n1, collect(nodes) AS event_node_list
          UNWIND range(0, size(event_node_list)-2) AS i
          RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
          "WITH n1,e1,e2
          MERGE (e1)-[df :DF1 {{EntityType:'AT_Pro7'}}]->(e2)",
          {{batchSize:1000}})'''
  runQuery(driver, query_create_directly_follows)
  query_create_directly_follows = f'''
          CALL apoc.periodic.iterate(
          "MATCH (n1:Entity) WHERE n1.ID='{'Project8'}'
          MATCH (n1)<-[:CORR]-(e:Event)
          WITH n1, e AS nodes ORDER BY e.Date
          WITH n1, collect(nodes) AS event_node_list
          UNWIND range(0, size(event_node_list)-2) AS i
          RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
          "WITH n1,e1,e2
          MERGE (e1)-[df :DF1 {{EntityType:'AT_Pro8'}}]->(e2)",
          {{batchSize:1000}})'''
  runQuery(driver, query_create_directly_follows)
  query_create_directly_follows = f'''
          CALL apoc.periodic.iterate(
          "MATCH (n1:Entity) WHERE n1.ID='{'Project9'}'
          MATCH (n1)<-[:CORR]-(e:Event)
          WITH n1, e AS nodes ORDER BY e.Date
          WITH n1, collect(nodes) AS event_node_list
          UNWIND range(0, size(event_node_list)-2) AS i
          RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
          "WITH n1,e1,e2
          MERGE (e1)-[df :DF1 {{EntityType:'AT_Pro9'}}]->(e2)",
          {{batchSize:1000}})'''
  runQuery(driver, query_create_directly_follows)
  query_create_directly_follows = f'''
          CALL apoc.periodic.iterate(
          "MATCH (n1:Entity) WHERE n1.ID='{'Project10'}'
          MATCH (n1)<-[:CORR]-(e:Event)
          WITH n1, e AS nodes ORDER BY e.Date
          WITH n1, collect(nodes) AS event_node_list
          UNWIND range(0, size(event_node_list)-2) AS i
          RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
          "WITH n1,e1,e2
          MERGE (e1)-[df :DF1 {{EntityType:'AT_Pro10'}}]->(e2)",
          {{batchSize:1000}})'''
  runQuery(driver, query_create_directly_follows)





