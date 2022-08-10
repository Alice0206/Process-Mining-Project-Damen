# First we create start and end time for each event class
import pandas as pd
data = pd.read_csv("cleaned_data.csv")
res = {}

# find start and end time of each event class
for index, row in data.iterrows():
    Project = row['Project']
    Activity = row['Activity']
    Person = row['Person']
    Date = pd.to_datetime(row['Date'], format = '%d/%m/%Y')
    Hours = row['Hours']
    if Project not in res:
        res[Project] = {}
    if Activity not in res[Project]:
        res[Project][Activity] = {}
    if Person not in res[Project][Activity]:
        res[Project][Activity][Person] = {'Start': '30/12/2022',
        'End': '01/01/2015','Hours': 0}
    if Date < pd.to_datetime(res[Project][Activity][Person]['Start']):
        res[Project][Activity_Number][Person]['Start'] = Date
    if Date > pd.to_datetime(res[Project][Activity][Person]['End']):
        res[Project][Activity][Person]['End'] = Date
        res[Project][Activity][Person]['Hours'] += float(Hours)
            print('Project,Activity,Person,Start,End,Hours')
        
for Project in res:
    for Activity in res[Project]:
        for Person in res[Project][Activity]:
            str_ = 
            str(Project) + ','+ str(Activity) +',' + str(Person) + ','+ 
            str(res[Project][Activity][Person]['Start']) + ',' + 
            str(res[Project][Activity][Person]['End']) +',' + 
            str(res[Project][Activity][Person]['Hours'])
            print(str_)
#----------------------------------------- Use Excel and Power Query in the middle--------------------------------

# Then we add the start time and end time for each event based on the event classifier in the data via Excel and pivot based on the project, activity and person, 
# afterwards, unpivot the table on the combination of the three values in Power Query.
# We use the unpivoted dataset to generate waiting nodes as follows.
------------------------------------------------------------------------------------------------------------------
from datetime import date
# import holidays library to select waiting dates
import holidays
data = pd.read_csv('unpivot.csv')
data = data.loc[data['Activity'] == 'AT1']

nl_holidays = holidays.NL()
def ho_weekends():
    for index, row in data.iterrows():
        Date = pd.to_datetime(row['Time'], format = '%Y-%m-%d')
        if Date in nl_holidays:
            print(Date)
    for index, row in data.iterrows():
        Date = pd.to_datetime(row['Time'], format = '%Y-%m-%d')
        if Date.weekday() > 4:
            print(Date)
        
ho_weekends()
data_normal = data.loc[-data['Date'].isin(ho_weekends)]
data_anomly = data.loc[data['Date'].isin(ho_weekends)]
data_anomly = data_anomly[data_anomly['Hours'] > 0]
data2 = pd.concat([data_normal, data_anomly]).sort_values('Date') 

# select events in the interval of an event class
mask = (data2['Date'] >= data2['Start_Time']) &
(data['Date'] <= data2['End_Time'])
data2 = data2.loc[mask]
AT1 = data2["Activity" == "AT1"]
AT1.to_csv("AT1.csv")
