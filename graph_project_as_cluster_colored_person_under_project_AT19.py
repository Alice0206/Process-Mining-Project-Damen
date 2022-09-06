#!/usr/bin/env python
# coding: utf-8

# In[1]:


from neo4j import GraphDatabase
from graphviz import Digraph
import graphviz
from datetime import datetime
import numpy as np
import os


# In[2]:


### begin config
# connection to Neo4J database
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))
# os.environ['PATH'].split(os.pathsep)
# os.environ['PATH'] += os.pathsep + 'D:\\anaconda\\Library\\bin\\graphviz'


# In[3]:


##### colors
#c81919 - dark red
#f9cccc - light red
#
#0333a3 - dark blue
#bbd1ff - light blue
#
#feb729 - yellow
#fed47f - light yellow
#
#178544 - dark green
#4ae087 - light green
#
#a034a8 - purple
#e7bdeb - light purple
#
#13857d - dark cyan
#19b1a7 - cyan
#93f0ea - light cyan

c2_cyan = "#318599"
c2_orange = "#ea700d"
c2_light_orange = "#f59d56"
c2_light_yellow = "#ffd965"

c3_light_blue = "#5b9bd5"
c3_red = "#ff0000"
c3_green = "#70ad47"
c3_yellow = "#ffc000"


c4_red = '#d7191c'
c4_orange = '#fdae61'
c4_yellow = '#ffffbf'
c4_light_blue = '#abd9e9'
c4_dark_blue = '#2c7bb6'

c_white = "#ffffff"
c_black = "#000000"

c5_red = '#d73027'
c5_orange = '#fc8d59'
c5_yellow = '#fee090'
c5_light_blue = '#e0f3f8'
c5_medium_blue = '#91bfdb'
c5_dark_blue = '#4575b4'




# In[4]:


# Crimson 
c1 =  "#DC143C" 
# HotPink  
c2 = "#FF69B4"
# DeepPink 
c3 = "#FF1493"
# Pink 
c4 = "#FFC0CB"
# MediumVioletRed 
c5 = "#C71585"
# Orchid 
c6 = "#DA70D6"
# Thistle 
c7 = "#D8BFD8"
# plum 
c8 = "#DDA0DD"
# Violet 
c9 = "#EE82EE"
# Magenta 
c10 = "#FF00FF"
# Fuchsia 
c11 = "#FF00FF"
# DarkMagenta 
c12 = "#8B008B"
# Purple 
c13 = "#800080"
# MediumOrchid 
c14 = "#BA55D3"
# DarkVoilet 
c15 = "#9400D3"
# DarkOrchid 
c16 = "#9932CC"
# Indigo 
c17 = "#4B0082"
# BlueViolet 
c18 = "#8A2BE2"



# RosyBrown
c19 = "#BC8F8F"

# IndianRed
c20 = "#CD5C5C"

# Red
c21 = "#FF0000"

# Brown
c22 = "#A52A2A"


# FireBrick
c23 = "#B22222"

# DarkRed
c24 = "#8B0000"




c2_cyan = "#318599"
c2_orange = "#ea700d"
c2_light_orange = "#f59d56"
c2_light_yellow = "#ffd965"

c3_light_blue = "#5b9bd5"
c3_red = "#ff0000"
c3_green = "#70ad47"
c3_yellow = "#ffc000"


c4_red = '#d7191c'
c4_orange = '#fdae61'
c4_yellow = '#ffffbf'
c4_light_blue = '#abd9e9'
c4_dark_blue = '#2c7bb6'

c_white = "#ffffff"
c_black = "#000000"

c5_red = '#d73027'
c5_orange = '#fc8d59'
c5_yellow = '#fee090'
c5_light_blue = '#e0f3f8'
c5_medium_blue = '#91bfdb'
c5_dark_blue = '#4575b4'





# Aqua	
"#00FFFF"


# Baby Blue	
C1 = "#89CFF0"
# Blue	
C2 = "#0000FF"
# Blue Gray	
C3 = "#7393B3"

# Blue Green	
C4 = "#088F8F"

# Bright Blue	
C5 = "#0096FF"

# Cadet Blue	
C6 = "#5F9EA0"

# Cobalt Blue	
C7 = "#0047AB"

# Cornflower Blue	
C8 = "#6495ED"

# Cyan	
C9 = "#00FFFF"

# Dark Blue	
C10 = "#00008B"

# Denim	
C11 = "#6F8FAF"

# Egyptian Blue	
C12 = "#1434A4"

# Electric Blue	
C13 = "#7DF9FF"

# Glaucous	
C14 = "#6082B6"

# Jade	
C15 = "#00A36C"

# Indigo	
C16 = "#3F00FF"

# # Iris	
C17 =  "#5D3FD3"

# # Light Blue	
C18 = "#ADD8E6"

# # Midnight Blue	
C19 = "#191970"

# # Navy Blue	
C20 = "#000080"

# # Neon Blue	
C21 = "#1F51FF"

# Pastel Blue	
C22 = "#A7C7E7"

# Periwinkle	
C23 = "#CCCCFF"

# Powder Blue	
C24 = "#B6D0E2"

# Robin Egg Blue	
C25 = "#96DED1"

# Royal Blue	
C26 = "#4169E1"

# Sapphire Blue	
C27 = "#0F52BA"

# Seafoam Green	
C28 = "#9FE2BF"

# Sky Blue	
c29 = "#87CEEB"

# Steel Blue	
#4682B4

# Teal	
#008080

# Turquoise	
#40E0D0

# Ultramarine	
#0437F2

# Verdigris	
#40B5AD

# Zaffre	
#0818A8


# In[5]:


#background color

c5_medium_blue = '#8091bfdb'
c5_dark_blue = '#804575b4'
# Baby Blue	
C1 = "#8089CFF0"
# Blue	
C2 = "#800000FF"
# Blue Gray	
C3 = "#807393B3"

# Blue Green	
C4 = "#80088F8F"

# Bright Blue	
C5 = "#800096FF"

# Cadet Blue	
C6 = "#805F9EA0"

# Cobalt Blue	
C7 = "#800047AB"

# Cornflower Blue	
C8 = "#806495ED"

# Cyan	
C9 = "#8000FFFF"

# Dark Blue	
C10 = "#8000008B"

# Denim	
C11 = "#806F8FAF"

# Egyptian Blue	
C12 = "#801434A4"

# Electric Blue	
C13 = "#807DF9FF"

# Glaucous	
C14 = "#806082B6"

# Jade	
C15 = "#8000A36C"

# Indigo	
C16 = "#803F00FF"

# # Iris	
C17 =  "#805D3FD3"

# # Light Blue	
C18 = "#80ADD8E6"

# # Midnight Blue	
C19 = "#80191970"

# # Navy Blue	
C20 = "#80000080"

# # Neon Blue	
C21 = "#801F51FF"

# Pastel Blue	
C22 = "#80A7C7E7"

# Periwinkle	
C23 = "#80CCCCFF"

# Powder Blue	
C24 = "#80B6D0E2"

# Robin Egg Blue	
C25 = "#8096DED1"

# Royal Blue	
C26 = "#804169E1"

# Sapphire Blue	
C27 = "#800F52BA"

# Seafoam Green	
C28 = "#809FE2BF"

# Sky Blue	
c29 = "#8087CEEB"


# In[6]:



holidays = [
"2016-03-25",
"2016-05-16" ,
"2017-04-14" ,
"2017-04-27" ,
"2017-05-25" ,
"2018-03-30" ,
"2018-04-02" ,
"2018-04-27" ,
"2018-05-10" ,
"2018-05-21" ,
"2019-04-19" ,
"2019-05-30" ,
"2019-06-09" ,
"2019-06-10" ,
"2020-04-10" ,
"2020-04-27" ,
"2020-05-05" ,
"2020-06-01" ,
"2021-04-02" ,
"2021-04-05" ]

# all activities
AT_selector = 'True'

# 9 randomly selected cases showing variety of behavior
Projects = ['Project2','Project7']
# Projects = ['Project1','Project2','Project3','Project4','Project5','Project6','Project7','Project8','Project9','Project10','Project11','Project12','Project13','Project14','Project15','Project16','Project17','Project19','Project25','Project26']
Pro_selector = "e1.Project in "+str(Projects)
Pro_selector_e2 = "e2.Project in "+str(Projects)


def getNodeLabel_Event(name):
    return name[7:14]
#     return name[:]


# In[7]:


def getEventsDF(tx, dot, entity_type,entity, color, fontcolor, edge_width):
    q = f'''
        MATCH (e1) -[r:DF1{{EntityType:"{entity_type}"}}]-> (e2:Event)                        
        RETURN e1,r,e2
        '''
    print(q)
   
    for record in tx.run(q):
#         print(record)
        if record["e2"] != None:
            e1_date = str(record["e1"]["Date"])
            e1_name = str(record["e1"]["Date"])+ ' P'  + getNodeLabel_Event(str(record["e1"]["Project"]))+' '+ getNodeLabel_Event(str(record["e1"]["Person"]))
#             e1_name_1 = str(record["e1"]["Date"])+ ' P'  + getNodeLabel_Event(str(record["e1"]["Project"]))+' '+ getNodeLabel_Event(str(record["e1"]["Person"]))
            e2_date = str(record["e2"]["Date"])
            e2_name = str(record["e2"]["Date"])+ ' P'  + getNodeLabel_Event(str(record["e2"]["Project"]))+' '+ getNodeLabel_Event(str(record["e2"]["Person"]))     
#             e2_name_1 = str(record["e2"]["Date"])+ ' P'  + getNodeLabel_Event(str(record["e2"]["Project"]))+' '+ getNodeLabel_Event(str(record["e2"]["Person"])) 
            e1_label = ' P'  + getNodeLabel_Event(str(record["e1"]["Project"]))+' '+ getNodeLabel_Event(str(record["e1"]["Person"]))
            e2_label = ' P'  + getNodeLabel_Event(str(record["e2"]["Project"]))+' '+ getNodeLabel_Event(str(record["e2"]["Person"]))                                                                                            
         
            e1_project = str(record['e1']['Project'])
            e2_project = str(record['e2']['Project'])
            
#             days = np.busday_count(record['e1']['Date'], record['e2']['Date'], holidays = holidays)
            with dot.subgraph(name = 'cluster' + e1_project) as c:
                c.attr(style = "filled", fillcolor = color)
                c.node(e1_project, label = e1_project,shape="rectangle",fixedsize="false", width="0.4", height="0.4", style = "filled", fillcolor = color, fontcolor = "white")
                cluster_name_e1 = 'cluster' + e1_date + e1_project
                cluster_name_e2 = 'cluster' + e2_date + e2_project
                if e1_date == e2_date:
                    with c.subgraph(name=cluster_name_e1) as a:
                        if int(record['e1']['Value']) == 0:
                            a.node(e1_name, style='invis')
                            a.attr(style='invis')
                           
                        else:
                            a.attr(style='invis')
                            a.node(e1_name,label = e1_name,style = "filled", fillcolor = c_white,fontcolor = fontcolor)
                        
                        if int(record['e2']['Value']) == 0:
                            a.attr(style='invis')
                            a.node(e2_name, style='invis')
                        else:
                            a.attr(style='invis')
                            a.node(e2_name,label = e2_name,style = "filled", fillcolor = c_white,fontcolor = fontcolor)
                         
                
                else:
                    with c.subgraph(name= cluster_name_e1) as a: 
#                         a.node(cluster_name_e1,label = "", style='invis',**{'width':str(0), 'height':str(0)})
                        if int(record['e1']['Value']) == 0:
                            a.node(e1_name, style='invis')
                            a.attr(style='invis')
                        else:
                            a.attr(style='invis')
                            a.node(e1_name,label = e1_name,style = "filled", fillcolor = c_white,fontcolor = fontcolor)
        
                    with c.subgraph(name= cluster_name_e2) as a:  
#                         a.node(cluster_name_e2,label = "", style='invis',**{'width':str(0), 'height':str(0)})
                        if int(record['e2']['Value']) == 0:
                            a.node(e2_name,style='invis')
                            a.attr(style='invis')
                        else:
                            a.attr(style='invis')
                            a.node(e2_name,label = e2_name,style = "filled", fillcolor = c_white,fontcolor = fontcolor)
                
                    pen_width = str(edge_width)
                    edge_color = c_white
                    dot.edge(e1_name, e2_name, style = 'invis')
                    dot.attr(rankdir = 'LR')


# In[8]:


def getPersonDF(tx, dot, entity_type):
    q = f'''
        match (n:Entity {{EntityType:"{'Person'}"}}) <-[:CORR]- (e1:Event) -[r:DF6{{EntityType:'{entity_type}'}}]-> (e2:Event)
        return e1,r,e2,n
        '''
    for record in tx.run(q):
        if record["e2"] != None:
            e1_date = str(record["e1"]["Date"])
            e1_name = str(record["e1"]["Date"])+ ' P'  + getNodeLabel_Event(str(record["e1"]["Project"]))+' '+ getNodeLabel_Event(str(record["e1"]["Person"]))
            e2_date = str(record["e2"]["Date"])
            e2_name = str(record["e2"]["Date"])+ ' P'  + getNodeLabel_Event(str(record["e2"]["Project"]))+' '+ getNodeLabel_Event(str(record["e2"]["Person"])) 
            e1_person = str(record['e1']['Person'])
            e2_person = str(record['e2']['Person'])

            dot.edge(e1_name, e2_name, rank = "same",style = "invis")
            dot.attr(rankdir = "LR")


# In[9]:


def getActivityDF(tx, dot):
    q = f'''
        MATCH (n:Entity {{EntityType:"{'Activity'}"}}) <-[:CORR]- (e1:Event) -[r:DF{{EntityType:'Activity'}}]-> (e2:Event)
        WHERE {Pro_selector} AND {Pro_selector_e2}
        return e1,r,e2,n
        '''
    for record in tx.run(q):
        if record["e2"] != None:
            e1_date = str(record["e1"]["Date"])
            e1_name = str(record["e1"]["Date"])+ ' P'  + getNodeLabel_Event(str(record["e1"]["Project"]))+' '+ getNodeLabel_Event(str(record["e1"]["Person"]))
            e2_date = str(record["e2"]["Date"])
            e2_name = str(record["e2"]["Date"])+ ' P'  + getNodeLabel_Event(str(record["e2"]["Project"]))+' '+ getNodeLabel_Event(str(record["e2"]["Person"])) 
            e1_person = str(record['e1']['Person'])
            e2_person = str(record['e2']['Person'])
            e1_label = ' P'  + getNodeLabel_Event(str(record["e1"]["Project"]))+' '+ getNodeLabel_Event(str(record["e1"]["Person"]))
            e2_label = ' P'  + getNodeLabel_Event(str(record["e2"]["Project"]))+' '+ getNodeLabel_Event(str(record["e2"]["Person"]))                                                                                                      
            dot.node(e1_name, style = "invis")
            dot.node(e2_name, style = "invis")
            if e1_date == e2_date:
                dot.edge(e1_name,e2_name,style = "invis",constraint = "false")    
            else:
                dot.edge(e1_name,e2_name,style = "invis") 
                dot.attr(rankdir = 'LR')


# In[10]:


def getResourcesDF(tx, dot, ID, color, fontcolor, edge_width):
    q = f'''
        MATCH (n:Entity {{ID:"{ID}"}}) <-[:CORR]- (e1:Event) -[r:DF3{{EntityType:'AT_Person'}}]-> (e2:Event)
        WHERE {Pro_selector} AND {Pro_selector_e2}
        return e1,r,e2,n
        '''
    for record in tx.run(q):
        if record["e2"] != None:
            e1_date = str(record["e1"]["Date"])
            e1_name = str(record["e1"]["Date"])+ ' P'  + getNodeLabel_Event(str(record["e1"]["Project"]))+' '+ getNodeLabel_Event(str(record["e1"]["Person"]))
            e2_date = str(record["e2"]["Date"])
            e2_name = str(record["e2"]["Date"])+ ' P'  + getNodeLabel_Event(str(record["e2"]["Project"]))+' '+ getNodeLabel_Event(str(record["e2"]["Person"])) 
            e1_person = str(record['e1']['Person'])
            e2_person = str(record['e2']['Person'])
            e1_label = ' P'  + getNodeLabel_Event(str(record["e1"]["Project"]))+' '+ getNodeLabel_Event(str(record["e1"]["Person"]))
            e2_label = ' P'  + getNodeLabel_Event(str(record["e2"]["Project"]))+' '+ getNodeLabel_Event(str(record["e2"]["Person"]))                                                                                                      
            days = np.busday_count(record['e1']['Date'], record['e2']['Date'], holidays = holidays)
            if e1_date == e2_date:
                dot.node(e1_name,style='filled',fillcolor=color,fontcolor = fontcolor)
                dot.node(e2_name,style='filled',fillcolor=color,fontcolor = fontcolor)
                dot.edge(e1_name,e2_name,constraint = "false",color = color)
               
            else:
                dot.node(e1_name,style='filled',fillcolor=color,fontcolor = fontcolor)
                dot.node(e2_name,style='filled',fillcolor=color,fontcolor = fontcolor)
                if days == 0  or days == 1:
                    edge_label = "E"+ record["n"]["ID"][8:11]
                else:     
                    edge_label = "E"+ record["n"]["ID"][8:11] + "__"+str(int(days)-1) + ' days'
                pen_width = str(edge_width)
                edge_color = color
                dot.edge(e1_name,e2_name,xlabel=edge_label,color=edge_color,penwidth=pen_width,fontname="Helvetica", fontsize="10",fontcolor=edge_color) 
                dot.attr(rankdir = 'LR')


# In[11]:


def getProjectsDF(tx, dot, edge_width):
    q = f'''
        match (n:Entity {{EntityType:"Project"}}) <-[:CORR]- (e1:Event) -[r:DF4{{EntityType:'AT_Project'}}]-> (e2:Event)
        WHERE {Pro_selector} AND {Pro_selector_e2}
        return e1,r,e2,n
        '''
    for record in tx.run(q):
        if record["e2"] != None:
            e1_date = str(record["e1"]["Date"])
            e1_name = str(record["e1"]["Date"])+ ' P'  + getNodeLabel_Event(str(record["e1"]["Project"]))+' '+ getNodeLabel_Event(str(record["e1"]["Person"]))
            e2_date = str(record["e2"]["Date"])
            e2_name = str(record["e2"]["Date"])+ ' P'  + getNodeLabel_Event(str(record["e2"]["Project"]))+' '+ getNodeLabel_Event(str(record["e2"]["Person"])) 
            e1_person = str(record['e1']['Person'])
            e2_person = str(record['e2']['Person'])
            e1_project = str(record['e1']['Project'])
            e2_project = str(record['e2']['Project'])
            e1_label = ' P'  + getNodeLabel_Event(str(record["e1"]["Project"]))+' '+ getNodeLabel_Event(str(record["e1"]["Person"]))
            e2_label = ' P'  + getNodeLabel_Event(str(record["e2"]["Project"]))+' '+ getNodeLabel_Event(str(record["e2"]["Person"]))                                                                                                      
            if e1_date == e2_date:
                pen_width = str(edge_width)
#                 edge_color = c_black
                edge_color = c2_cyan
                edge_label = "P"+ record["n"]["ID"][7:9]
                dot.edge(e1_name, e2_name,constraint='false',color=edge_color)

            else:
                days = np.busday_count(record['e1']['Date'], record['e2']['Date'], holidays = holidays)
                if days == 0 or days == 1:
                    edge_label = "P"+ record["n"]["ID"][7:9]
                else:
                    edge_label = "P"+ record["n"]["ID"][7:9] + '__' +str(int(days)-1) + ' days'
    #             xlabel = record["n"]["ID"]
                pen_width = str(edge_width)
                edge_color = c2_cyan
#                 edge_color = c_black
        #         dot.node(str(record["e1"]['Date'])),color=c5_red,penwidth="2")
        #         dot.node(str(record["e2"]['Date'])),color=c5_red,penwidth="2")
                cluster_name_e1 = 'cluster' + e1_date + e1_project
                cluster_name_e2 = 'cluster' + e2_date + e2_project
                dot.edge(e1_name, e2_name,constraint='false',xlabel=edge_label,color=edge_color,penwidth=pen_width,fontname="Helvetica", fontsize="8",fontcolor=edge_color)
                dot.attr(rankdir= 'LR')


# In[12]:


def getProjectForFirstEvent(tx,dot,ID,color,fontcolor):
    q = f'''
        MATCH (e1:Event) -[c:CORR]-> (n:Entity)
        WHERE n.ID = "{ID}" AND NOT (:Event)-[:DF{{EntityType:n.EntityType}}]->(e1) AND {Pro_selector}
        return e1,c,n
        '''
    print(q)

#     dot.attr("node",shape="rectangle",fixedsize="false", width="0.4", height="0.4", fontname="Helvetica", fontsize="8", margin="0")
    for record in tx.run(q):
        
        e_date = str(record["e1"]['Date'])
        e_name = str(record["e1"]["Date"])+ ' P'  + getNodeLabel_Event(str(record["e1"]["Project"]))+' '+ getNodeLabel_Event(str(record["e1"]["Person"]))           
#         e_name = getNodeLabel_Event(record["e"]["Activity"])
        entity_type = record["n"]["EntityType"]
        
        entity_id = record["n"]["ID"]
#         entity_uid = record["n"]["id"]
        entity_label = entity_type+'\n' +entity_id
        
        dot.node(entity_id, entity_label,shape="rectangle",fixedsize="false", width="0.4", height="0.4",color=color, style="filled", fillcolor=color, fontcolor=fontcolor)
        dot.edge(entity_id, e_name, style="dashed", arrowhead="none",color=color)
def getPersonForFirstEvent(tx,dot,ID,color,fontcolor):
    q = f'''
        MATCH (e1:Event) -[c:CORR]-> (n:Entity)
        WHERE n.ID = "{ID}" AND NOT (:Event)-[:DF{{EntityType:n.EntityType}}]->(e1) AND {Pro_selector}
        return e1,c,n
        '''
    print(q)

#     dot.attr("node",shape="rectangle",fixedsize="false", width="0.4", height="0.4", fontname="Helvetica", fontsize="8", margin="0")
    for record in tx.run(q):
        
        e_date = str(record["e1"]['Date'])
        e_name = str(record["e1"]["Date"])+ ' P'  + getNodeLabel_Event(str(record["e1"]["Project"]))+' '+ getNodeLabel_Event(str(record["e1"]["Person"]))           
#         e_name = getNodeLabel_Event(record["e"]["Activity"])
        entity_type = record["n"]["EntityType"]
        
        entity_id = record["n"]["ID"]
#         entity_uid = record["n"]["id"]
        entity_label = entity_type+'\n' +entity_id
        
        dot.node(entity_id, entity_label,shape="rectangle",fixedsize="false", width="0.4", height="0.4",color=color, style="filled", fillcolor=color, fontcolor=fontcolor)
        dot.edge(entity_id, e_name, style="dashed", arrowhead="none",color=color)


# In[13]:


def getEntityForFirstEvent(tx,dot,entity_type,color,fontcolor):
    q = f'''
        MATCH (e1:Event) -[c:CORR]-> (n:Entity)
        WHERE n.EntityType = "{entity_type}" AND NOT (:Event)-[:DF{{EntityType:n.EntityType}}]->(e1) AND {Pro_selector}
        return e1,c,n
        '''
    print(q)

#     dot.attr("node",shape="rectangle",fixedsize="false", width="0.4", height="0.4", fontname="Helvetica", fontsize="8", margin="0")
    for record in tx.run(q):
        e_date = str(record["e1"]['Date'])
        e_name = str(record["e1"]["Date"])+ ' P'  + getNodeLabel_Event(str(record["e1"]["Project"]))+' '+ getNodeLabel_Event(str(record["e1"]["Person"]))           
#         e_name = getNodeLabel_Event(record["e"]["Activity"])
        entity_type = record["n"]["EntityType"]
        
        entity_id = record["n"]["ID"]
#         entity_uid = record["n"]["id"]
        entity_label = entity_type+'\n' +entity_id
        
        dot.node(entity_id, entity_label,shape="rectangle",fixedsize="false", width="0.4", height="0.4",color=color, style="filled", fillcolor=color, fontcolor=fontcolor)
        dot.edge(entity_id, e_name, style="dashed", arrowhead="none",color=color)


# In[16]:



dot = Digraph("G",comment='Query Result')
dot.attr("graph",margin="0")
# dot.attr("graph",rankdir="LR",margin="0")
with driver.session() as session:
#     session.read_transaction(getDF, dot, 'Activity',c5_orange,c_black,3)
    session.read_transaction(getActivityDF, dot) 
    session.read_transaction(getEventsDF, dot,"AT_Pro2","Project" , C1, c_black, 3)
    session.read_transaction(getPersonDF,dot,"Pro2_Person")
#     session.read_transaction(getProjectForFirstEvent, dot, "Project1",c5_dark_blue,c_white)
    
    session.read_transaction(getEventsDF, dot,"AT_Pro7","Project" , C3, c_black, 3)
    session.read_transaction(getPersonDF,dot,"Pro7_Person")
#     session.read_transaction(getProjectForFirstEvent, dot, "Project4",c5_medium_blue,c_white)  
    

    


    

# 136  166   46   68   74   83
    
    session.read_transaction(getResourcesDF, dot, "Employee136", c1, c_black,3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee136",c1,c_white)
    
    session.read_transaction(getResourcesDF, dot, "Employee166", c2, c_black,3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee166",c2,c_white)
    
    session.read_transaction(getResourcesDF, dot, "Employee46", c16,c_black, 3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee46",c16,c_white)
    
    session.read_transaction(getResourcesDF, dot, "Employee68", c3, c_black,3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee68",c3,c_white)
    
    session.read_transaction(getResourcesDF, dot, "Employee74", c5, c_black,3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee74",c5,c_white)
    
    session.read_transaction(getResourcesDF, dot, "Employee83", c6, c_black,3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee83",c6,c_white)

    


#     session.read_transaction(getEventsDF, dot,  "Project","AT_Pro1", c5_dark_blue, c_white, 3)
#     session.read_transaction(getEventsDF, dot, "Person", c5_medium_blue, c_black, 3)
#     session.read_transaction(getResourcesDF, dot,c5_red, 3) 
    
#     session.read_transaction(getProjectsDF, dot,3)

    session.read_transaction(getEntityForFirstEvent, dot, "Activity",c5_orange,c_black)
#     session.read_transaction(getEntityForFirstEvent, dot, "Project",c5_dark_blue,c_white)
#     session.read_transaction(getEntityForFirstEvent, dot, "Person",c5_red,c_white)
    
#print(dot.source)
file = open("activities.dot","w") 
file.write(dot.source)
file.close()
dot.render('test-output/round-table.gv',view = "true")


# In[ ]:





# In[ ]:




