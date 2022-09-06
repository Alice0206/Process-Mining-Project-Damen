#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import time, os, csv
from neo4j import GraphDatabase


# In[2]:


driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))


# In[3]:


def runQuery(driver, query):
    print('\n'+query)
    with driver.session() as session:
        result = session.run(query).single()
        if result != None: 
            return result.value()
        else:
            return None


# In[4]:


sample = True
if(sample):
    inputFile = 'AT4.csv' 
else:
    inputFile = 'person_cleaned.csv'


# In[5]:


inputPath = 'D:/TUe/2nd_Quarter/Damen/graph/'
fullInputPath = os.path.realpath(inputPath+inputFile)


# In[6]:


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


# In[7]:


def CreateEventQuery(logHeader, fileName, LogID = ""):
    query = f'USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM \"file:/{fileName}\" as line'
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


# In[8]:


qCleanDatabase_allRelations = f'''
CALL apoc.periodic.iterate(
"MATCH ()-[r]->() RETURN id(r) AS id",
"MATCH ()-[r]->() WHERE id(r)=id DELETE r",
{{batchSize:10000}})''' 


# In[9]:


qCleanDatabase_allNodes = f'''
CALL apoc.periodic.iterate(
"MATCH (n) RETURN id(n) AS id",
"MATCH (n) WHERE id(n)=id DELETE n",
{{batchSize:10000}})'''


# In[10]:


runQuery(driver, qCleanDatabase_allRelations) # delete all relationships, comment out if you want to keep them
runQuery(driver, qCleanDatabase_allNodes)     # delete all nodes, comment out if you want to keep them


# In[11]:


header, csvLog = LoadLog(inputPath+inputFile)


# In[12]:


qCreateEvents = CreateEventQuery(header, "AT4.csv", 'Event')
qCreateEvents


# In[13]:


runQuery(driver, qCreateEvents)


# In[14]:


#### Step 1.c) example of querying for the number of event nodes in the DB
q_countImportedEvents = "MATCH (e:Event) RETURN count(e)"
result = runQuery(driver, q_countImportedEvents) 
print (result)


# In[15]:


####################################################
#### Step 2: delete events with suspend and resume life-cycle, comment out to keep these events
####################################################
periodic_commit = True #change to False id you do not want periodic iterations


# In[16]:


q_countImportedEvents = "MATCH (e:Event) RETURN count(e)"
result = runQuery(driver, q_countImportedEvents) 
print (result)


# In[17]:


####################################################
#### Step 3: create entity nodes and correlate events to entity nodes
####################################################
data_entities = ['Activity','Project','Person']

if periodic_commit:
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

else:
    for entity in data_entities:
        query_create_entity_nodes = f'''
            MATCH (e:Event) 
            WITH DISTINCT e.{entity} AS id
            CREATE (n:Entity {{ID:id, EntityType:"{entity}"}})'''
        runQuery(driver, query_create_entity_nodes)

        query_correlate_events_to_entity = f'''
            MATCH (e:Event) WHERE EXISTS(e.{entity})
            MATCH (n:Entity {{EntityType: "{entity}"}}) WHERE e.{entity} = n.ID
            CREATE (e)-[:CORR]->(n)'''
        runQuery(driver, query_correlate_events_to_entity)


# In[18]:


####################################################
#### Step 3: create entity nodes and correlate events to entity nodes
####################################################
# data_entities = ['Activity','Project','Person']
# A_ID = ['AT1']
# # ,'AT2','AT3','AT4','AT5','AT6','AT7','AT1','AT1','AT1','AT1',
# #         'AT1','AT1','AT1','AT1','AT1','AT1','AT1','AT1','AT1','AT1','AT1',
# #         'AT1','AT1','AT1','AT1','AT1','AT1','AT1','AT1',]
# P_ID = ['Project1','Project2','Project3','Project4','Project5','Project6',
#         'Project7','Project8','Project9','Project10','Project11','Project12',
#         'Project13','Project14','Project15','Project16','Project17','Project18',
#         'Project19','Project25','Project26']
# if periodic_commit:
#     for activity_id in A_ID:
#         query_create_entity_nodes = f'''
#             CALL apoc.periodic.iterate(
#             "MATCH (e:Event) WITH DISTINCT e.{activity_id} AS id RETURN id",
#             "CREATE (n:Entity {{ID:'{activity_id}', EntityType:'Activity'}})",
#             {{batchSize:10000}})'''
#         runQuery(driver, query_create_entity_nodes)
        
#         query_correlate_events_to_entity = f'''
#             CALL apoc.periodic.iterate(
#             "MATCH (e:Event) WHERE EXISTS(e.{activity_id}) MATCH (n:Entity {{ID: '{activity_id}'}}) WHERE e.{activity_id} = n.ID RETURN id(e) AS IDe, id(n) as IDn",
#             "MATCH (e:Event) WHERE id(e)=IDe MATCH (n:Entity) WHERE id(n) = IDn CREATE (e)-[:CORR]->(n)",
#             {{batchSize:10000}})'''
#         runQuery(driver, query_correlate_events_to_entity)
#     for project_id in P_ID:
#         query_correlate_events_to_entity = f'''
#             CALL apoc.periodic.iterate(
#             "MATCH (e:Event) WHERE EXISTS(e.{project_id}) MATCH (n:Entity {{ID: '{project_id}'}}) WHERE e.{project_id} = n.ID RETURN id(e) AS IDe, id(n) as IDn",
#             "MATCH (e:Event) WHERE id(e)=IDe MATCH (n:Entity) WHERE id(n) = IDn CREATE (e)-[:CORR]->(n)",
#             {{batchSize:10000}})'''
#         runQuery(driver, query_correlate_events_to_entity)

# else:
#     for entity in data_entities:
#         query_create_entity_nodes = f'''
#             MATCH (e:Event) 
#             WITH DISTINCT e.{entity} AS id
#             CREATE (n:Entity {{ID:id, EntityType:"{entity}"}})'''
#         runQuery(driver, query_create_entity_nodes)

#         query_correlate_events_to_entity = f'''
#             MATCH (e:Event) WHERE EXISTS(e.{entity})
#             MATCH (n:Entity {{EntityType: "{entity}"}}) WHERE e.{entity} = n.ID
#             CREATE (e)-[:CORR]->(n)'''
#         runQuery(driver, query_correlate_events_to_entity)


# In[19]:


####################################################
#### Step 3: create directly-follows DF relationship
####################################################

if periodic_commit:
    for entity in data_entities:
        query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (n:Entity) WHERE n.EntityType='{entity}'
            MATCH (n)<-[:CORR]-(e:Event)
            WITH n, e AS nodes ORDER BY e.Date
            WITH n, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n,e1,e2
            MERGE (e1)-[df:DF {{EntityType:n.EntityType}}]->(e2)",
            {{batchSize:1000}})'''
        runQuery(driver, query_create_directly_follows)
    

else:
    for entity in data_entities:
        query_create_directly_follows = f'''
            MATCH (n:Entity) WHERE n.EntityType="{entity}"
            MATCH (n)<-[:CORR]-(e)
            WITH n, e AS nodes ORDER BY e.Date
            WITH n, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            WITH n, event_node_list[i] AS e1, event_node_list[i+1] AS e2
            MERGE (e1)-[df:DF {{EntityType:n.EntityType}}]->(e2)'''
        runQuery(driver, query_create_directly_follows)

q_countDFrelations = "MATCH (:Event)-[r:DF]->(:Event) RETURN count(r)"
result = runQuery(driver, q_countDFrelations)
print (result)


# In[20]:


####################################################
#### Step 3: create directly-follows DF1 relationship
####################################################
Projects_ID = ['Project1','Project2','Project3','Project4','Project5','Project6','Project7','Project8','Project9','Project10','Project11','Project12','Project13','Project14','Project15','Project16','Project17','Project18','Project19','Project25','Project26']

if periodic_commit:
#     for ID in Projects_ID:

    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project4'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        WITH n1, e AS nodes ORDER BY e.Date
        WITH n1, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n1,e1,e2
        MERGE (e1)-[df1 :DF1 {{EntityType:'AT_Pro4'}}]->(e2)",
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
            MERGE (e1)-[df1 :DF1 {{EntityType:'AT_Pro2'}}]->(e2)",
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
            MERGE (e1)-[df1 :DF1 {{EntityType:'AT_Pro3'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows) 
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (n1:Entity) WHERE n1.ID='{'Project5'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df1 :DF1 {{EntityType:'AT_Pro5'}}]->(e2)",
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
            MERGE (e1)-[df1 :DF1 {{EntityType:'AT_Pro6'}}]->(e2)",
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
            MERGE (e1)-[df1 :DF1 {{EntityType:'AT_Pro7'}}]->(e2)",
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
            MERGE (e1)-[df1 :DF1 {{EntityType:'AT_Pro8'}}]->(e2)",
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
            MERGE (e1)-[df1 :DF1 {{EntityType:'AT_Pro9'}}]->(e2)",
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
            MERGE (e1)-[df1 :DF1 {{EntityType:'AT_Pro10'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (n1:Entity) WHERE n1.ID='{'Project11'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df1 :DF1 {{EntityType:'AT_Pro11'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (n1:Entity) WHERE n1.ID='{'Project12'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df1 :DF1 {{EntityType:'AT_Pro12'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (n1:Entity) WHERE n1.ID='{'Project13'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df1 :DF1 {{EntityType:'AT_Pro13'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (n1:Entity) WHERE n1.ID='{'Project14'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df1 :DF1 {{EntityType:'AT_Pro14'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (n1:Entity) WHERE n1.ID='{'Project15'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df1 :DF1 {{EntityType:'AT_Pro15'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (n1:Entity) WHERE n1.ID='{'Project16'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df1 :DF1 {{EntityType:'AT_Pro16'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (n1:Entity) WHERE n1.ID='{'Project17'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df1 :DF1 {{EntityType:'AT_Pro17'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (n1:Entity) WHERE n1.ID='{'Project19'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df1 :DF1 {{EntityType:'AT_Pro19'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (n1:Entity) WHERE n1.ID='{'Project25'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df1 :DF1 {{EntityType:'AT_Pro25'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (n1:Entity) WHERE n1.ID='{'Project26'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df1 :DF1 {{EntityType:'AT_Pro26'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    
# # if periodic_commit:    
# #     query_create_directly_follows = f'''
# #             CALL apoc.periodic.iterate(
# #             "MATCH (n:Entity) WHERE n.ID='{'AT1'}'
# #             MATCH (n)<-[:CORR]-(e:Event)
# #             MATCH (n1:Entity) WHERE n1.ID='{'Project1'}'
# #             MATCH (n1)<-[:CORR]-(e:Event)
# #             MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
# #             MATCH (n2)<-[:CORR]-(e:Event)
# #             WITH n2, e AS nodes ORDER BY e.Date
# #             WITH n2, collect(nodes) AS event_node_list
# #             UNWIND range(0, size(event_node_list)-2) AS i
# #             RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
# #             "WITH n2,e1,e2
# #             MERGE (e1)-[df1 :DF1 {{EntityType:'AT1_Pro1_Person'}}]->(e2)",
# #             {{batchSize:1000}})'''
# # #     runQuery(driver, query_create_directly_follows)
# # if periodic_commit:    
# #     query_create_directly_follows = f'''
# #             CALL apoc.periodic.iterate(
# #             "MATCH (n:Entity) WHERE n.ID='{'AT1'}'
# #             MATCH (n)<-[:CORR]-(e:Event)
# #             MATCH (n1:Entity) WHERE n1.ID='{'Project4'}'
# #             MATCH (n1)<-[:CORR]-(e:Event)
# #             MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
# #             MATCH (n2)<-[:CORR]-(e:Event)
# #             WITH n2, e AS nodes ORDER BY e.Date
# #             WITH n2, collect(nodes) AS event_node_list
# #             UNWIND range(0, size(event_node_list)-2) AS i
# #             RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
# #             "WITH n2,e1,e2
# #             MERGE (e1)-[df1 :DF1 {{EntityType:'AT1_Pro4_Person'}}]->(e2)",
# #             {{batchSize:1000}})'''
# #     runQuery(driver, query_create_directly_follows)
else:
    for entity in data_entities:
        query_create_directly_follows = f'''
            MATCH (n:Entity) WHERE n.EntityType="{entity}"
            MATCH (n)<-[:CORR]-(e)
            WITH n, e AS nodes ORDER BY e.Date
            WITH n, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            WITH n, event_node_list[i] AS e1, event_node_list[i+1] AS e2
            MERGE (e1)-[df:DF {{EntityType:n.EntityType}}]->(e2)'''
        runQuery(driver, query_create_directly_follows)

q_countDF1relations = "MATCH (:Event)-[r:DF1]->(:Event) RETURN count(r)"
result = runQuery(driver, q_countDF1relations)
print (result)






# In[21]:


####################################################
#### Step 3: create directly-follows DF1 relationship
####################################################
if periodic_commit:   
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
            MERGE (e1)-[df2 :DF2 {{EntityType:'AT_Pro4'}}]->(e2)",
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
            MERGE (e1)-[df2 :DF2 {{EntityType:'AT_Pro1'}}]->(e2)",
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
            MERGE (e1)-[df2 :DF2 {{EntityType:'AT_Pro2'}}]->(e2)",
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
            MERGE (e1)-[df2 :DF2 {{EntityType:'AT_Pro3'}}]->(e2)",
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
            MERGE (e1)-[df2 :DF2 {{EntityType:'AT_Pro5'}}]->(e2)",
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
            MERGE (e1)-[df2 :DF2 {{EntityType:'AT_Pro6'}}]->(e2)",
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
            MERGE (e1)-[df2 :DF2 {{EntityType:'AT_Pro7'}}]->(e2)",
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
            MERGE (e1)-[df2 :DF2 {{EntityType:'AT_Pro8'}}]->(e2)",
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
            MERGE (e1)-[df2 :DF2 {{EntityType:'AT_Pro9'}}]->(e2)",
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
            MERGE (e1)-[df2 :DF2 {{EntityType:'AT_Pro10'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (e:Event) WHERE e.Value = '{'1'}'
            MATCH (n1:Entity) WHERE n1.ID='{'Project11'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df2 :DF2 {{EntityType:'AT_Pro11'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (e:Event) WHERE e.Value = '{'1'}'
            MATCH (n1:Entity) WHERE n1.ID='{'Project12'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df2 :DF2 {{EntityType:'AT_Pro12'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (e:Event) WHERE e.Value = '{'1'}'
            MATCH (n1:Entity) WHERE n1.ID='{'Project13'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df2 :DF2 {{EntityType:'AT_Pro13'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (e:Event) WHERE e.Value = '{'1'}'
            MATCH (n1:Entity) WHERE n1.ID='{'Project14'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df2 :DF2 {{EntityType:'AT_Pro14'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (e:Event) WHERE e.Value = '{'1'}'
            MATCH (n1:Entity) WHERE n1.ID='{'Project15'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df2 :DF2 {{EntityType:'AT_Pro15'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (e:Event) WHERE e.Value = '{'1'}'
            MATCH (n1:Entity) WHERE n1.ID='{'Project16'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df2 :DF2 {{EntityType:'AT_Pro16'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (e:Event) WHERE e.Value = '{'1'}'
            MATCH (n1:Entity) WHERE n1.ID='{'Project17'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df2 :DF2 {{EntityType:'AT_Pro17'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (e:Event) WHERE e.Value = '{'1'}'
            MATCH (n1:Entity) WHERE n1.ID='{'Project19'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df2 :DF2 {{EntityType:'AT_Pro19'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (e:Event) WHERE e.Value = '{'1'}'
            MATCH (n1:Entity) WHERE n1.ID='{'Project25'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df2 :DF2 {{EntityType:'AT_Pro25'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
            CALL apoc.periodic.iterate(
            "MATCH (e:Event) WHERE e.Value = '{'1'}'
            MATCH (n1:Entity) WHERE n1.ID='{'Project26'}'
            MATCH (n1)<-[:CORR]-(e:Event)
            WITH n1, e AS nodes ORDER BY e.Date
            WITH n1, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
            "WITH n1,e1,e2
            MERGE (e1)-[df2 :DF2 {{EntityType:'AT_Pro26'}}]->(e2)",
            {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
else:
    for entity in data_entities:
        query_create_directly_follows = f'''
            MATCH (n:Entity) WHERE n.EntityType="{entity}"
            MATCH (n)<-[:CORR]-(e)
            WITH n, e AS nodes ORDER BY e.Date
            WITH n, collect(nodes) AS event_node_list
            UNWIND range(0, size(event_node_list)-2) AS i
            WITH n, event_node_list[i] AS e1, event_node_list[i+1] AS e2
            MERGE (e1)-[df:DF {{EntityType:n.EntityType}}]->(e2)'''
        runQuery(driver, query_create_directly_follows)

q_countDF2relations = "MATCH (:Event)-[r:DF2]->(:Event) RETURN count(r)"
result = runQuery(driver, q_countDF2relations)
print (result)






# In[22]:


if periodic_commit:   
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.EntityType='{'Activity'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        WITH n1, e AS nodes ORDER BY e.Date
        WITH n1, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n1,e1,e2
        MERGE (e1)-[df3 :DF3 {{EntityType:'AT_Activity'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)  

q_countDF3relations = "MATCH (:Event)-[r:DF3]->(:Event) RETURN count(r)"
result = runQuery(driver, q_countDF3relations)
print (result)


# In[23]:


if periodic_commit:   
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.EntityType='{'Person'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        WITH n1, e AS nodes ORDER BY e.Date
        WITH n1, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n1,e1,e2
        MERGE (e1)-[df3 :DF3 {{EntityType:'AT_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)  

q_countDF3relations = "MATCH (:Event)-[r:DF3]->(:Event) RETURN count(r)"
result = runQuery(driver, q_countDF3relations)
print (result)


# In[24]:


if periodic_commit:   
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.EntityType='{'Project'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        WITH n1, e AS nodes ORDER BY e.Date
        WITH n1, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n1, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n1,e1,e2
        MERGE (e1)-[df3 :DF4 {{EntityType:'AT_Project'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)  

q_countDF4relations = "MATCH (:Event)-[r:DF4]->(:Event) RETURN count(r)"
result = runQuery(driver, q_countDF4relations)
print (result)


# In[25]:


if periodic_commit:   
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.ID='{'Project1'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df5 :DF5 {{EntityType:'Pro1_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows) 
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.ID='{'Project4'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df5 :DF5 {{EntityType:'Pro4_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.ID='{'Project2'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df5 :DF5 {{EntityType:'Pro2_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.ID='{'Project3'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df5 :DF5 {{EntityType:'Pro3_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.ID='{'Project5'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df5 :DF5 {{EntityType:'Pro5_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.ID='{'Project6'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df5 :DF5 {{EntityType:'Pro6_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.ID='{'Project7'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df5 :DF5 {{EntityType:'Pro7_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.ID='{'Project8'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df5 :DF5 {{EntityType:'Pro8_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.ID='{'Project9'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df5 :DF5 {{EntityType:'Pro9_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.ID='{'Project10'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df5 :DF5 {{EntityType:'Pro10_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.ID='{'Project11'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df5 :DF5 {{EntityType:'Pro11_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.ID='{'Project12'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df5 :DF5 {{EntityType:'Pro12_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.ID='{'Project13'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df5 :DF5 {{EntityType:'Pro13_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.ID='{'Project14'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df5 :DF5 {{EntityType:'Pro14_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.ID='{'Project15'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df5 :DF5 {{EntityType:'Pro15_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.ID='{'Project16'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df5 :DF5 {{EntityType:'Pro16_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.ID='{'Project17'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df5 :DF5 {{EntityType:'Pro17_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.ID='{'Project19'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df5 :DF5 {{EntityType:'Pro19_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.ID='{'Project25'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df5 :DF5 {{EntityType:'Pro25_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (e:Event) WHERE e.Value = '{'1'}'
        MATCH (n1:Entity) WHERE n1.ID='{'Project26'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df5 :DF5 {{EntityType:'Pro26_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    


q_countDF5relations = "MATCH (:Event)-[r:DF5]->(:Event) RETURN count(r)"
result = runQuery(driver, q_countDF5relations)
print (result)


# In[26]:


if periodic_commit:   
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project1'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df6 :DF6 {{EntityType:'Pro1_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows) 
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project4'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df6 :DF6 {{EntityType:'Pro4_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project2'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df6 :DF6 {{EntityType:'Pro2_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project3'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df6 :DF6 {{EntityType:'Pro3_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project5'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df6 :DF6 {{EntityType:'Pro5_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project6'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df6 :DF6 {{EntityType:'Pro6_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project7'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df6 :DF6 {{EntityType:'Pro7_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project8'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df6 :DF6 {{EntityType:'Pro8_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project9'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df6 :DF6 {{EntityType:'Pro9_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project10'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df6 :DF6 {{EntityType:'Pro10_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project11'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df6 :DF6 {{EntityType:'Pro11_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project12'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df6 :DF6 {{EntityType:'Pro12_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project13'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df6 :DF6 {{EntityType:'Pro13_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project14'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df6 :DF6 {{EntityType:'Pro14_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project15'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df6 :DF6 {{EntityType:'Pro15_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project16'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df6 :DF6 {{EntityType:'Pro16_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project17'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df6 :DF6 {{EntityType:'Pro17_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project19'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df6 :DF6 {{EntityType:'Pro19_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project25'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df6 :DF6 {{EntityType:'Pro25_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)
    query_create_directly_follows = f'''
        CALL apoc.periodic.iterate(
        "MATCH (n1:Entity) WHERE n1.ID='{'Project26'}'
        MATCH (n1)<-[:CORR]-(e:Event)
        MATCH (n2:Entity) WHERE n2.EntityType='{'Person'}'
        MATCH (n2)<-[:CORR]-(e:Event)
        WITH n2, e AS nodes ORDER BY e.Date
        WITH n2, collect(nodes) AS event_node_list
        UNWIND range(0, size(event_node_list)-2) AS i
        RETURN n2, event_node_list[i] AS e1, event_node_list[i+1] AS e2",
        "WITH n2,e1,e2
        MERGE (e1)-[df6 :DF6 {{EntityType:'Pro26_Person'}}]->(e2)",
        {{batchSize:1000}})'''
    runQuery(driver, query_create_directly_follows)

q_countDF6relations = "MATCH (:Event)-[r:DF6]->(:Event) RETURN count(r)"
result = runQuery(driver, q_countDF6relations)
print (result)


# In[ ]:




