from datetime import datetime

from neo4j import GraphDatabase
from graphviz import Digraph
import graphviz
import numpy as np
import pandas as pd
import holidays

# begin config
# connection to Neo4J database
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))

# colors

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

 
c1 = "#DC143C" # Crimson
c2 = "#FF69B4" # HotPink
c3 = "#FF1493" # DeepPink
c4 = "#FFC0CB" # Pink
c5 = "#C71585" # MediumVioletRed
c6 = "#DA70D6" # Orchid
c7 = "#D8BFD8" # Thistle
c8 = "#DDA0DD" # plum
c9 = "#EE82EE" # Violet
c10 = "#FF00FF" # Magenta
c11 = "#FF00FF" # Fuchsia
c12 = "#8B008B" # DarkMagenta
c13 = "#800080" # Purple
c14 = "#BA55D3" # MediumOrchid
c15 = "#9400D3" # DarkVoilet
c16 = "#9932CC" # DarkOrchid
c17 = "#4B0082" # Indigo
c18 = "#8A2BE2" # BlueViolet
c19 = "#BC8F8F" # RosyBrown
c20 = "#CD5C5C" # IndianRed
c21 = "#FF0000" # Red
c22 = "#A52A2A" # Brown
c23 = "#B22222" # FireBrick
c24 = "#8B0000" # DarkRed

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

C0 = "#00FFFF" # Aqua
C1 = "#89CFF0" # Baby Blue
C2 = "#0000FF" # Blue
C3 = "#7393B3" # Blue Gray
C4 = "#088F8F" # Blue Green
C5 = "#0096FF" # Bright Blue
C6 = "#5F9EA0" # Cadet Blue
C7 = "#0047AB" # Cobalt Blue
C8 = "#6495ED" # Cornflower Blue
C9 = "#00FFFF" # Cyan
C10 = "#00008B" # Dark Blue
C11 = "#6F8FAF" # Denim
C12 = "#1434A4" # Egyptian Blue
C13 = "#7DF9FF" # Electric Blue
C14 = "#6082B6" # Glaucous
C15 = "#00A36C" # Jade
C16 = "#3F00FF" # Indigo
C17 =  "#5D3FD3" # Iris
C18 = "#ADD8E6" # Light Blue
C19 = "#191970" # Midnight Blue
C20 = "#000080" # Navy Blue
C21 = "#1F51FF" # Neon Blue
C22 = "#A7C7E7" # Pastel Blue
C23 = "#CCCCFF" # Periwinkle
C24 = "#B6D0E2" # Powder Blue
C25 = "#96DED1" # Robin Egg Blue
C26 = "#4169E1" # Royal Blue
C27 = "#0F52BA" # Sapphire Blue
C28 = "#9FE2BF" # Seafoam Green
c29 = "#87CEEB" # Sky Blue

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


# date_person_as_cluster

start, end = "2016-01-01", "2021-12-31"
holiday_dates = [str(date.date()) for date in pd.date_range(start, end, freq="1d") if date in holidays.NL()]


# 9 randomly selected cases showing variety of behavior
# Projects = ['Project1','Project4']
Projects = ['Project1','Project2','Project3','Project4','Project5','Project6','Project7','Project8','Project9','Project10']
Pro_selector = "e1.Project in " + str(Projects)
Pro_selector_e2 = "e2.Project in " + str(Projects)


def get_node_label_event(name):
    return name[7:14]
#     return name[:]

def get_events_df(tx, dot, entity_type, color, fontcolor, edge_width, verbose: bool = False):
    q = f'''
        MATCH (e1) -[r:DF2{{EntityType:"{entity_type}"}}]-> (e2:Event)
        RETURN e1,r,e2
        '''
    if verbose:
        print(q)
    
    for record in tx.run(q):
        if record["e2"] != None:
            e1_id = str(record['e1'].id)
            e2_id = str(record['e2'].id)
            e1_date = str(record["e1"]["Date"])
            e1_name = str(record["e1"]["Date"]) + ' P' + get_node_label_event(str(record["e1"]["Project"])) + ' ' + get_node_label_event(str(record["e1"]["Person"]))
            e2_date = str(record["e2"]["Date"])
            e2_name = str(record["e2"]["Date"]) + ' P' + get_node_label_event(str(record["e2"]["Project"])) + ' ' + get_node_label_event(str(record["e2"]["Person"])) 
            e1_label = ' P'  + get_node_label_event(str(record["e1"]["Project"]))+' '+ get_node_label_event(str(record["e1"]["Person"]))
            e2_label = ' P'  + get_node_label_event(str(record["e2"]["Project"]))+' '+ get_node_label_event(str(record["e2"]["Person"]))                                                                                            
            e1_project = str(record['e1']['Project'])
            e2_project = str(record['e2']['Project'])
            days = np.busday_count(record['e1']['Date'], record['e2']['Date'], holidays = holiday_dates)
            
            if e1_date == e2_date:
                edge_label = ""
                with dot.subgraph(name='cluster' + e1_date) as c:
#                 with dot.subgraph(name='cluster' + e1_date ) as c:
#                     c.attr(fontcolor='white')
#                     c.attr('node', style='filled', fillcolor=color, fontcolor=fontcolor)
                    c.node(e1_date)
                    with c.subgraph (name='cluster' + e1_date + e1_project) as a:
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
#                     c.attr(fontcolor='white')
                    c.node(e1_date)
                    with c.subgraph (name='cluster' + e1_date + e1_project) as a:
                        a.attr(style='filled', color= color)
#                         a.attr(style='filled', color= color)
#                         a.node(e1_name,label = e1_label)
                        a.node(e1_name,style='filled',fillcolor="white", fontcolor=fontcolor,label = e1_label)
                with dot.subgraph(name='cluster' + e2_date) as c:
#                     c.attr(fontcolor='white')
                    c.node(e2_date)
                    with c.subgraph (name='cluster' + e2_date + e2_project) as a:
                        a.attr(style='filled', color= color)
#                         a.attr(style='filled', color= color)
#                         a.node(e2_name,label = e2_label)
                        a.node(e2_name,style='filled',fillcolor="white", fontcolor=fontcolor,label = e2_label)
                pen_width = str(edge_width)
                edge_color = color
                dot.edge(e1_name, e2_name,constraint = "false",xlabel=edge_label,color=edge_color,penwidth=pen_width,fontname="Helvetica", fontsize="8",fontcolor=edge_color) 
                dot.attr(rankdir="LR")
                
# def getPersonDF(tx, dot, entity_type):
#     q = f'''
#         match (n:Entity {{EntityType:"{'Person'}"}}) <-[:CORR]- (e1:Event) -[r:DF5{{EntityType:'{entity_type}'}}]-> (e2:Event)
#         return e1,r,e2,n
#         '''
#     for record in tx.run(q):
#         if record["e2"] != None:
#             e1_date = str(record["e1"]["Date"])
#             e1_name = str(record["e1"]["Date"])+ ' P'  + get_node_label_event(str(record["e1"]["Project"]))+' '+ get_node_label_event(str(record["e1"]["Person"]))
#             e2_date = str(record["e2"]["Date"])
#             e2_name = str(record["e2"]["Date"])+ ' P'  + get_node_label_event(str(record["e2"]["Project"]))+' '+ get_node_label_event(str(record["e2"]["Person"])) 
#             e1_person = str(record['e1']['Person'])
#             e2_person = str(record['e2']['Person'])
#             dot.edge(e1_name, e2_name, rank = "same",style = "invis",constraint = "false")
#             dot.attr(rankdir = "LR")

            
            
            
def get_resources_df(tx, dot, ID, color, fontcolor, edge_width):
    q = f'''
        match (n:Entity {{ID:"{ID}"}}) <-[:CORR]- (e1:Event) -[r:DF3{{EntityType:'AT_Person'}}]-> (e2:Event)
        WHERE {Pro_selector} AND {Pro_selector_e2}
        return e1,r,e2,n
        '''
    for record in tx.run(q):
        if record["e2"] != None:
            e1_date = str(record["e1"]["Date"])
            e1_name = str(record["e1"]["Date"])+ ' P'  + get_node_label_event(str(record["e1"]["Project"]))+' '+ get_node_label_event(str(record["e1"]["Person"]))
            e2_date = str(record["e2"]["Date"])
            e2_name = str(record["e2"]["Date"])+ ' P'  + get_node_label_event(str(record["e2"]["Project"]))+' '+ get_node_label_event(str(record["e2"]["Person"])) 
            e1_person = str(record['e1']['Person'])
            e2_person = str(record['e2']['Person'])
            e1_label = ' P'  + get_node_label_event(str(record["e1"]["Project"]))+' '+ get_node_label_event(str(record["e1"]["Person"]))
            e2_label = ' P'  + get_node_label_event(str(record["e2"]["Project"]))+' '+ get_node_label_event(str(record["e2"]["Person"]))                                                                                                      
            days = np.busday_count(record['e1']['Date'], record['e2']['Date'], holidays = holiday_dates)
            if e1_date == e2_date:
                with dot.subgraph(name='cluster' + e1_date) as c:
#                     c.attr(fontcolor='white')
                    c.node(e1_date)
                    c.node(e1_name,style='filled',fillcolor=color,label = e1_label,fontcolor = fontcolor)
                    c.node(e2_name,style='filled',fillcolor=color,label = e2_label,fontcolor = fontcolor)
                dot.edge(e1_name,e2_name,constraint = "false",color = color)
            else: 
                with dot.subgraph(name='cluster' + e1_date) as c:
                    c.node(e1_date,fontcolor='black')
                    c.node(e1_name,style='filled',fillcolor=color, fontcolor=fontcolor,label = e1_label)
                with dot.subgraph(name='cluster' + e2_date) as c:
                    c.node(e2_date,fontcolor='black')
                    c.node(e2_name,style='filled',fillcolor=color, fontcolor=fontcolor,label = e2_label)
                if days == 0 or days == 1:
                    edge_label = "E"+ record["n"]["ID"][8:11]
                else:   
                    edge_label = "E"+ record["n"]["ID"][8:11] + "__"+str(int(days)-1) + ' days'
                pen_width = str(edge_width)
                dot.edge(e1_name,e2_name,constraint = "false",xlabel=edge_label,color=color,penwidth=pen_width,fontname="Helvetica", fontsize="8",fontcolor=color) 
                dot.attr(rankdir = "LR")

            


def get_entity_for_first_event(tx,dot,entity_type,color,fontcolor):
    q = f'''
        MATCH (e1:Event) -[c:CORR]-> (n:Entity)
        WHERE n.EntityType = "{entity_type}" AND NOT (:Event)-[:DF{{EntityType:n.EntityType}}]->(e1) AND {Pro_selector}
        return e1,c,n
        '''
    print(q)

#     dot.attr("node",shape="rectangle",fixedsize="false", width="0.4", height="0.4", fontname="Helvetica", fontsize="8", margin="0")
    for record in tx.run(q):
        e_date = str(record["e1"]['Date'])
        e_name = str(record["e1"]["Date"])+ ' P'  + get_node_label_event(str(record["e1"]["Project"]))+' '+ get_node_label_event(str(record["e1"]["Person"]))           
#         e_name = get_node_label_event(record["e"]["Activity"])
        entity_type = record["n"]["EntityType"]
        entity_id = record["n"]["ID"]
#         entity_uid = record["n"]["id"]
        entity_label = entity_type+'\n' +entity_id
        
        dot.node(entity_id, entity_label,shape="rectangle",color=color, fixedsize="false", width="0.4", height="0.4",style="filled", fillcolor=color, fontcolor=fontcolor)
        dot.edge(entity_id, e_name, style="dashed", arrowhead="none",color=color)

def get_project_for_first_event(tx,dot,ID,color,fontcolor):
    q = f'''
        MATCH (e1:Event) -[c:CORR]-> (n:Entity)
        WHERE n.ID = "{ID}" AND NOT (:Event)-[:DF{{EntityType:n.EntityType}}]->(e1) AND {Pro_selector}
        return e1,c,n
        '''
    print(q)

#     dot.attr("node",shape="rectangle",fixedsize="false", width="0.4", height="0.4", fontname="Helvetica", fontsize="8", margin="0")
    for record in tx.run(q):
        
        e_date = str(record["e1"]['Date'])
        e_name = str(record["e1"]["Date"])+ ' P'  + get_node_label_event(str(record["e1"]["Project"]))+' '+ get_node_label_event(str(record["e1"]["Person"]))           
#         e_name = get_node_label_event(record["e"]["Activity"])
        entity_type = record["n"]["EntityType"]
        
        entity_id = record["n"]["ID"]
#         entity_uid = record["n"]["id"]
        entity_label = entity_type+'\n' +entity_id
        
        dot.node(entity_id, entity_label,shape="rectangle",color=color ,fixedsize="false", width="0.4", height="0.4", style="filled", fillcolor=color, fontcolor=fontcolor)
        dot.edge(entity_id, e_name, style="dashed", arrowhead="none",color=color)
        
def get_person_for_first_event(tx,dot,ID,color,fontcolor):
    q = f'''
        MATCH (e1:Event) -[c:CORR]-> (n:Entity)
        WHERE n.ID = "{ID}" AND NOT (:Event)-[:DF{{EntityType:n.EntityType}}]->(e1) AND {Pro_selector}
        return e1,c,n
        '''
    print(q)

#     dot.attr("node",shape="rectangle",fixedsize="false", width="0.4", height="0.4", fontname="Helvetica", fontsize="8", margin="0")
    for record in tx.run(q):
        
        e_date = str(record["e1"]['Date'])
        e_name = str(record["e1"]["Date"])+ ' P'  + get_node_label_event(str(record["e1"]["Project"]))+' '+ get_node_label_event(str(record["e1"]["Person"]))           
#         e_name = get_node_label_event(record["e"]["Activity"])
        entity_type = record["n"]["EntityType"]
        
        entity_id = record["n"]["ID"]
#         entity_uid = record["n"]["id"]
        entity_label = entity_type+'\n' +entity_id
        
        dot.node(entity_id, entity_label,shape="rectangle",fixedsize="false", width="0.4", height="0.4",color=color, style="filled", fillcolor=color, fontcolor=fontcolor)
        dot.edge(entity_id, e_name, style="dashed", arrowhead="none",color=color)



def get_activity_df(tx, dot):
    q = f'''
        MATCH (e1:Event) -[r:DF3{{EntityType:'AT_Activity'}}]-> (e2:Event)
        return e1,r,e2
        '''
    for record in tx.run(q):
        if record["e2"] != None:
            e1_date = str(record["e1"]["Date"])
            e1_name = str(record["e1"]["Date"])+ ' P'  + get_node_label_event(str(record["e1"]["Project"]))+' '+ get_node_label_event(str(record["e1"]["Person"]))
            e2_date = str(record["e2"]["Date"])
            e2_name = str(record["e2"]["Date"])+ ' P'  + get_node_label_event(str(record["e2"]["Project"]))+' '+ get_node_label_event(str(record["e2"]["Person"])) 
            e1_person = str(record['e1']['Person'])
            e2_person = str(record['e2']['Person'])
            e1_label = ' P'  + get_node_label_event(str(record["e1"]["Project"]))+' '+ get_node_label_event(str(record["e1"]["Person"]))
            e2_label = ' P'  + get_node_label_event(str(record["e2"]["Project"]))+' '+ get_node_label_event(str(record["e2"]["Person"]))                                                                                                      
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
    session.read_transaction(get_events_df, dot,"AT_Pro1" , c5_dark_blue, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro1_Person")
    session.read_transaction(get_project_for_first_event, dot, "Project1",c5_dark_blue,c_white)
    
    session.read_transaction(get_events_df, dot,"AT_Pro4" , c5_medium_blue, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro4_Person")
    session.read_transaction(get_project_for_first_event, dot, "Project4",c5_medium_blue,c_white)  
    
    session.read_transaction(get_events_df, dot,"AT_Pro2" , C1, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro2_Person")
    session.read_transaction(get_project_for_first_event, dot, "Project2",C1,c_white)

    session.read_transaction(get_events_df, dot,"AT_Pro3" , C3, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro3_Person")
    session.read_transaction(get_project_for_first_event, dot, "Project3",C3,c_white)
    
    session.read_transaction(get_events_df, dot,"AT_Pro5" , C4, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro5_Person")
    session.read_transaction(get_project_for_first_event, dot, "Project5",C4,c_white)

    
    session.read_transaction(get_events_df, dot,"AT_Pro6" , C6, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro6_Person")
    session.read_transaction(get_project_for_first_event, dot, "Project6",C6,c_white)

    
    session.read_transaction(get_events_df, dot,"AT_Pro7" , C8, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro7_Person")
    session.read_transaction(get_project_for_first_event, dot, "Project7",C8,c_white)

    
    session.read_transaction(get_events_df, dot,"AT_Pro8" , C11, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro8_Person")
    session.read_transaction(get_project_for_first_event, dot, "Project8",C11,c_white)

    
    session.read_transaction(get_events_df, dot,"AT_Pro9" , C14, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro9_Person")
    session.read_transaction(get_project_for_first_event, dot, "Project9",C14,c_white)

    
    session.read_transaction(get_events_df, dot,"AT_Pro10", C15, c_black, 3)
#     session.read_transaction(getPersonDF,dot,"Pro10_Person")
    session.read_transaction(get_project_for_first_event, dot, "Project10",C15,c_white)
 

    
    session.read_transaction(get_resources_df, dot, "Employee55", c1, c_black,3)
    session.read_transaction(get_person_for_first_event, dot, "Employee55",c1,c_white)
    
    session.read_transaction(get_resources_df, dot, "Employee195", c2,c_black, 3)
    session.read_transaction(get_person_for_first_event, dot, "Employee195",c2,c_white)
    
    session.read_transaction(get_resources_df, dot, "Employee161", c16,c_black,3)
    session.read_transaction(get_person_for_first_event, dot, "Employee161",c16,c_white)
    
    session.read_transaction(get_resources_df, dot, "Employee216", c3,c_black,3)
    session.read_transaction(get_person_for_first_event, dot, "Employee216",c3,c_white)
    
    session.read_transaction(get_resources_df, dot, "Employee1", c5, c_black,3)
    session.read_transaction(get_person_for_first_event, dot, "Employee1",c5,c_white)
    
    session.read_transaction(get_resources_df, dot, "Employee231", c6,c_black, 3)
    session.read_transaction(get_person_for_first_event, dot, "Employee231",c6,c_white)

    session.read_transaction(get_resources_df, dot, "Employee256", c17,c_white, 3)
    session.read_transaction(get_person_for_first_event, dot, "Employee256",c17,c_white)

    session.read_transaction(get_resources_df, dot, "Employee23", c8,c_black,3)
    session.read_transaction(get_person_for_first_event, dot, "Employee23",c8,c_white)

    session.read_transaction(get_resources_df, dot, "Employee213", c9,c_black,3)
    session.read_transaction(get_person_for_first_event, dot, "Employee213",c9,c_white)

    session.read_transaction(get_resources_df, dot, "Employee78", c10, c_black,3)
    session.read_transaction(get_person_for_first_event, dot, "Employee78",c10,c_white)

    session.read_transaction(get_resources_df, dot, "Employee152", c11,c_black,3)
    session.read_transaction(get_person_for_first_event, dot, "Employee152",c11,c_white)

    session.read_transaction(get_resources_df, dot, "Employee62", c12,c_white, 3)
    session.read_transaction(get_person_for_first_event, dot, "Employee62",c12,c_white)
    
    session.read_transaction(get_resources_df, dot, "Employee42", c13,c_white,3)
    session.read_transaction(get_person_for_first_event, dot, "Employee42",c13,c_white)
    
    session.read_transaction(get_resources_df, dot, "Employee64", c14,c_black, 3)
    session.read_transaction(get_person_for_first_event, dot, "Employee64",c14,c_white)

    session.read_transaction(get_resources_df, dot, "Employee204", c15,c_black, 3)
    session.read_transaction(get_person_for_first_event, dot, "Employee204",c15,c_white)

    
    session.read_transaction(get_activity_df, dot)

    session.read_transaction(get_entity_for_first_event, dot, "Activity",c5_orange,c_black)
     
file = open("activities.dot","w") 
file.write(dot.source)
file.close()
dot.render('test-output/round-table.gv', view=True)
