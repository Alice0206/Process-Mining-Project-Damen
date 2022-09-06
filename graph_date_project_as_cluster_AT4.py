
from neo4j import GraphDatabase
from graphviz import Digraph
import graphviz
from datetime import datetime
import numpy as np



### begin config
# connection to Neo4J database
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))


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


#date_person_as_cluster
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
# Projects = ['Project1','Project4']
Projects = ['Project1','Project2','Project3','Project4','Project5','Project6','Project7','Project8','Project9','Project10','Project11','Project12','Project13','Project14','Project15','Project16','Project17','Project19','Project25','Project26']
Pro_selector = "e1.Project in "+str(Projects)
Pro_selector_e2 = "e2.Project in "+str(Projects)



def getNodeLabel_Event(name):
    return name[7:14]
#     return name[:]

def getEventsDF(tx, dot, entity_type, color, fontcolor, edge_width):
    q = f'''
        MATCH (e1) -[r:DF2{{EntityType:"{entity_type}"}}]-> (e2:Event)
        RETURN e1,r,e2
        '''
    print(q)
    
    i = 0
    for record in tx.run(q):
        if record["e2"] != None:
            e1_id = str(record['e1'].id)
            e2_id = str(record['e2'].id)
            e1_date = str(record["e1"]["Date"])
            e1_name = str(record["e1"]["Date"])+ ' P'  + getNodeLabel_Event(str(record["e1"]["Project"]))+' '+ getNodeLabel_Event(str(record["e1"]["Person"]))
            e2_date = str(record["e2"]["Date"])
            e2_name = str(record["e2"]["Date"])+ ' P'  + getNodeLabel_Event(str(record["e2"]["Project"]))+' '+ getNodeLabel_Event(str(record["e2"]["Person"])) 
            e1_label = ' P'  + getNodeLabel_Event(str(record["e1"]["Project"]))+' '+ getNodeLabel_Event(str(record["e1"]["Person"]))
            e2_label = ' P'  + getNodeLabel_Event(str(record["e2"]["Project"]))+' '+ getNodeLabel_Event(str(record["e2"]["Person"]))                                                                                            
            days = np.busday_count(record['e1']['Date'], record['e2']['Date'], holidays = holidays)
            if e1_date == e2_date:
                edge_label = ""
                with dot.subgraph(name='cluster' + e1_date) as c:
#                 with dot.subgraph(name='cluster' + e1_date ) as c:
#                     c.attr(fontcolor='white')
#                     c.attr('node', style='filled', fillcolor=color, fontcolor=fontcolor)
                    c.node(e1_date)
                    with c.subgraph (name='cluster' + e1_date + str(record['e1']['Project'])) as a:
                        a.attr(style='filled', color= color)
#                         a.node(e1_name,label = e1_label)
#                         a.node(e2_name,label = e2_label)
                        a.node(e1_name,style='filled',fillcolor="white", fontcolor=fontcolor,label = e1_label)
                        a.node(e2_name,style='filled',fillcolor="white", fontcolor=fontcolor,label = e2_label)
                
                pen_width = str(edge_width)
                edge_color = color
                dot.edge(e1_name, e2_name,constraint = "false",color=edge_color,penwidth=pen_width,fontname="Helvetica", fontsize="8",fontcolor=edge_color) 
                                      
            else:
                if days == 0 or days ==1:
                    edge_label = "P" + record["e1"]["Project"][7:9]
                else:
                    edge_label = "P" + record["e1"]["Project"][7:9] +"__"+ str(int(days)-1) + ' days'
                with dot.subgraph(name='cluster' + e1_date) as c:
                    c.attr(fontcolor='white')
                    c.node(e1_date)
                    with c.subgraph (name='cluster' + e1_date + str(record['e1']['Project'])) as a:
                        a.attr(style='filled', color= color)
#                         a.attr(style='filled', color= color)
#                         a.node(e1_name,label = e1_label)
                        a.node(e1_name,style='filled',fillcolor="white", fontcolor=fontcolor,label = e1_label)
                with dot.subgraph(name='cluster' + e2_date) as c:
#                     c.attr(fontcolor='white')
                    c.node(e2_date)
                    with c.subgraph (name='cluster' + e2_date + str(record['e2']['Project'])) as a:
                        a.attr(style='filled', color= color)
#                         a.attr(style='filled', color= color)
#                         a.node(e2_name,label = e2_label)
                        a.node(e2_name,style='filled',fillcolor="white", fontcolor=fontcolor,label = e2_label)
                pen_width = str(edge_width)
                edge_color = color
                dot.edge(e1_name, e2_name,xlabel=edge_label,color=edge_color,penwidth=pen_width,fontname="Helvetica", fontsize="8",fontcolor=edge_color) 
                dot.attr(rankdir="LR")
                
def getPersonDF(tx, dot, entity_type):
    q = f'''
        match (n:Entity {{EntityType:"{'Person'}"}}) <-[:CORR]- (e1:Event) -[r:DF5{{EntityType:'{entity_type}'}}]-> (e2:Event)
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
            
def getResourcesDF(tx, dot, ID, color, fontcolor, edge_width):
    q = f'''
        match (n:Entity {{ID:"{ID}"}}) <-[:CORR]- (e1:Event) -[r:DF3{{EntityType:'AT_Person'}}]-> (e2:Event)
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
                with dot.subgraph(name='cluster' + e1_date) as c:
#                     c.attr(fontcolor='white')
                    c.node(e1_date)
                    c.node(e1_name,style='filled',fillcolor=color,label = e1_label,fontcolor = fontcolor)
                    c.node(e2_name,style='filled',fillcolor=color,label = e2_label,fontcolor = fontcolor)
                dot.edge(e1_name,e2_name,constraint = "false",color = color)
            else: 

                dot.node(e1_name,style='filled',fillcolor=color,label = e1_label,fontcolor = fontcolor)
                dot.node(e2_name,style='filled',fillcolor=color,label = e2_label,fontcolor = fontcolor)
                if days == 0 or days == 1:
                    edge_label = "E"+ record["n"]["ID"][8:11]
                else:   
                    edge_label = "E"+ record["n"]["ID"][8:11] + "__"+str(int(days)-1) + ' days'
                pen_width = str(edge_width)
                dot.edge(e1_name,e2_name,constraint = "false",xlabel=edge_label,color=color,penwidth=pen_width,fontname="Helvetica", fontsize="8",fontcolor=color) 
                dot.attr(rankdir = "LR")



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
        
        dot.node(entity_id, entity_label,color=color, style="filled", fillcolor=color, fontcolor=fontcolor)
        dot.edge(entity_id, e_name, style="dashed", arrowhead="none",color=color)

        
        
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
        
        dot.node(entity_id, entity_label,shape="rectangle",color=color ,fixedsize="false", width="0.4", height="0.4", style="filled", fillcolor=color, fontcolor=fontcolor)
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
        
        
def getActivityDF(tx, dot):
    q = f'''
        MATCH (e1:Event) -[r:DF3{{EntityType:'AT_Activity'}}]-> (e2:Event)
        return e1,r,e2
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
#             dot.node(e1_name, label = e1_label)
#             dot.node(e2_name, label  = e2_label)
            if e1_date == e2_date:
                dot.edge(e1_name,e2_name,style = "invis",constraint = "false")    
            else:
                dot.edge(e1_name,e2_name,style = "invis") 
                dot.attr(rankdir = 'LR')

dot = Digraph("G",comment='Query Result')
dot.attr("graph",margin="0")

with driver.session() as session:
    session.read_transaction(getEventsDF, dot,"AT_Pro1" , c5_dark_blue, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro1_Person")
    session.read_transaction(getProjectForFirstEvent, dot, "Project1",c5_dark_blue,c_white)
    
    session.read_transaction(getEventsDF, dot,"AT_Pro4" , c5_medium_blue, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro4_Person")
    session.read_transaction(getProjectForFirstEvent, dot, "Project4",c5_medium_blue,c_white)  
    
    session.read_transaction(getEventsDF, dot,"AT_Pro2" , C1, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro2_Person")
    session.read_transaction(getProjectForFirstEvent, dot, "Project2",C1,c_white)

    session.read_transaction(getEventsDF, dot,"AT_Pro3" , C3, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro3_Person")
    session.read_transaction(getProjectForFirstEvent, dot, "Project3",C3,c_white)
    
    session.read_transaction(getEventsDF, dot,"AT_Pro5" , C4, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro5_Person")
    session.read_transaction(getProjectForFirstEvent, dot, "Project5",C4,c_white)

    
    session.read_transaction(getEventsDF, dot,"AT_Pro6" , C6, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro6_Person")
    session.read_transaction(getProjectForFirstEvent, dot, "Project6",C6,c_white)

    
    session.read_transaction(getEventsDF, dot,"AT_Pro7" , C8, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro7_Person")
    session.read_transaction(getProjectForFirstEvent, dot, "Project7",C8,c_white)

    
    session.read_transaction(getEventsDF, dot,"AT_Pro8" , C11, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro8_Person")
    session.read_transaction(getProjectForFirstEvent, dot, "Project8",C11,c_white)

    
    session.read_transaction(getEventsDF, dot,"AT_Pro9" , C14, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro9_Person")
    session.read_transaction(getProjectForFirstEvent, dot, "Project9",C14,c_white)

    
    session.read_transaction(getEventsDF, dot,"AT_Pro10", C15, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro10_Person")
    session.read_transaction(getProjectForFirstEvent, dot, "Project10",C15,c_white)

    session.read_transaction(getEventsDF, dot,"AT_Pro11" , C16, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro11_Person")
    session.read_transaction(getProjectForFirstEvent, dot, "Project11",C16,c_white)
    
    session.read_transaction(getEventsDF, dot,"AT_Pro12" , C17, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro12_Person")
    session.read_transaction(getProjectForFirstEvent, dot, "Project12",C17,c_white)
    
    session.read_transaction(getEventsDF, dot,"AT_Pro13" , C18, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro13_Person")
    session.read_transaction(getProjectForFirstEvent, dot, "Project13",C18,c_white)
    
    session.read_transaction(getEventsDF, dot,"AT_Pro14" , C19, c_white, 3)
#     session.read_transaction(getPersonDF,dot,"Pro14_Person")
    session.read_transaction(getProjectForFirstEvent, dot, "Project14",C19,c_white)
    
    session.read_transaction(getEventsDF, dot,"AT_Pro15" , C20, c_white, 3)
#     session.read_transaction(getPersonDF,dot,"Pro15_Person")
    session.read_transaction(getProjectForFirstEvent, dot, "Project15",C20,c_white)
    
    session.read_transaction(getEventsDF, dot,"AT_Pro16" , C21, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro16_Person")
    session.read_transaction(getProjectForFirstEvent, dot, "Project16",C21,c_white)
    
    session.read_transaction(getEventsDF, dot,"AT_Pro17" , C22, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro17_Person")
    session.read_transaction(getProjectForFirstEvent, dot, "Project17",C22,c_white)
    
    session.read_transaction(getEventsDF, dot,"AT_Pro19" , C23, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro19_Person")
    session.read_transaction(getProjectForFirstEvent, dot, "Project19",C23,c_white)
    
    session.read_transaction(getEventsDF, dot,"AT_Pro25" , C24, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro25_Person")
    session.read_transaction(getProjectForFirstEvent, dot, "Project25",C24,c_white)
    
    session.read_transaction(getEventsDF, dot,"AT_Pro26" , C25, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro26_Person")
    session.read_transaction(getProjectForFirstEvent, dot, "Project26",C26,c_white)
     
    
    session.read_transaction(getResourcesDF, dot, "Employee70", c1, c_black,3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee70",c1,c_white)
    
    session.read_transaction(getResourcesDF, dot, "Employee185", c2, c_black,3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee185",c2,c_white)
    
    session.read_transaction(getResourcesDF, dot, "Employee91", c16,c_black, 3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee91",c16,c_white)
    
    session.read_transaction(getResourcesDF, dot, "Employee113", c3, c_black,3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee113",c3,c_white)
    
    session.read_transaction(getResourcesDF, dot, "Employee133", c5, c_black,3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee133",c5,c_white)
    
    session.read_transaction(getResourcesDF, dot, "Employee140", c6, c_black,3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee140",c6,c_white)

    session.read_transaction(getResourcesDF, dot, "Employee6", c17,c_white, 3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee6",c17,c_white)

    session.read_transaction(getResourcesDF, dot, "Employee85", c8,c_black, 3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee85",c8,c_white)

    session.read_transaction(getResourcesDF, dot, "Employee22", c9,c_black, 3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee22",c9,c_white)

    session.read_transaction(getResourcesDF, dot, "Employee170", c10,c_black, 3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee170",c10,c_white)

    session.read_transaction(getResourcesDF, dot, "Employee141", c11, c_black,3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee141",c11,c_white)

    session.read_transaction(getResourcesDF, dot, "Employee258", c12, c_white,3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee258",c12,c_white)
    
    session.read_transaction(getResourcesDF, dot, "Employee33", c13,c_white, 3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee33",c13,c_white)
    
    session.read_transaction(getResourcesDF, dot, "Employee84", c14,c_black, 3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee84",c14,c_white)

    session.read_transaction(getResourcesDF, dot, "Employee69", c15, c_black,3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee69",c15,c_white)
    
    session.read_transaction(getResourcesDF, dot, "Employee182", c19,c_black, 3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee182",c19,c_white)

    session.read_transaction(getResourcesDF, dot, "Employee73", c20, c_black,3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee73",c20,c_white)

    session.read_transaction(getResourcesDF, dot, "Employee111", c21,c_white, 3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee111",c21,c_white)

    session.read_transaction(getResourcesDF, dot, "Employee158", c23,c_white, 3)
    session.read_transaction(getPersonForFirstEvent, dot, "Employee158",c23,c_white)

    session.read_transaction(getActivityDF, dot)
    
  
    session.read_transaction(getEntityForFirstEvent, dot, "Activity",c5_orange,c_black)
#     session.read_transaction(getEntityForFirstEvent, dot, "Project",c5_dark_blue,c_white)
#     session.read_transaction(getEntityForFirstEvent, dot, "Person",c5_medium_blue,c_black)
    
file = open("activities.dot","w") 
file.write(dot.source)
file.close()
dot.render('test-output/round-table.gv', view=True)


