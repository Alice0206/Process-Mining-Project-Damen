Hours = open('D:/TUe/2nd Quarter/Damen/code/project_hours_recovered.txt','r')
def transfer_time(old_time):
    year = old_time.split('/')[2]
    month = old_time.split('/')[0]
    if len(month) == 1:
        month = '0' + month
    day = old_time.split('/')[1]
    if len(day) == 1:
        day = '0' + day
    return year + '/' + month + '/' + day

res = {}
for line in Hours.readlines():
    Old_Date = line.split(',')[0]
    Date = transfer_time(Old_Date)
    Project = line.split(',')[1]
    Main_Activity_Number = line.split(',')[2]
    Activity_Number = line.split(',')[3]
    Person = line.split(',')[5]
    Hours = line.split(',')[6]
    Document_Number = line.split(',')[7]
    Document_Type = line.split(',')[8]
    Document_Revision = line.split(',')[9]
    if Project not in res:
        res[Project] = {}
    if Main_Activity_Number not in res[Project]:
        res[Project][Main_Activity_Number] = {}
    if Activity_Number not in res[Project][Main_Activity_Number]:
        res[Project][Main_Activity_Number][Activity_Number] = {}
    if Person not in res[Project][Main_Activity_Number][Activity_Number]:
        res[Project][Main_Activity_Number][Activity_Number][Person] = {'Start_Time':'2023/12/30', 'End_Time':'2015/1/1','Hours': 0}
    if Date < res[Project][Main_Activity_Number][Activity_Number][Person]['Start_Time']:
        res[Project][Main_Activity_Number][Activity_Number][Person]['Start_Time'] = Date
    if Date > res[Project][Main_Activity_Number][Activity_Number][Person]['End_Time']:
        res[Project][Main_Activity_Number][Activity_Number][Person]['End_Time'] = Date
    res[Project][Main_Activity_Number][Activity_Number][Person]['Hours'] += float(Hours)
#print(res)

for Project in res:
    for Main_Activity_Number in res[Project]:
        for Activity_Number in res[Project][Main_Activity_Number]:
            for Person in res[Project][Main_Activity_Number][Activity_Number]:
                str_ = Project + ' '+ Main_Activity_Number + ' '+ Activity_Number + ' ' + Person+' '+ res[Project][Main_Activity_Number][Activity_Number][Person]['Start_Time'] + ' ' + res[Project][Main_Activity_Number][Activity_Number][Person]['End_Time'] +' ' + str(res[Project][Main_Activity_Number][Activity_Number][Person]['Hours'])
                print(str_)

import pandas as pd

read_file = pd.read_csv (r'D:/TUe/2nd Quarter/Damen/code/Hours.txt')
read_file.to_csv (r'D:/TUe/2nd Quarter/Damen/code/Project.csv', index=None)










