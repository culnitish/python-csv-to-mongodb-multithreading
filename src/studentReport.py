#!/usr/bin/env python
import pymongo
import pandas as pd
import pymongo
from dbConnection import myCollection
cursor=myCollection.find({})
df =  pd.DataFrame(list(cursor))
# print(df.head())
df['total_marks'] = df['sub1']+df['sub2']+df['sub3']+df['sub4']+df['sub5']
groupByDepartment=df.groupby('department')
# print(groupByDepartment.max())
df=df.groupby(['department']).apply(lambda x : x.sort_values(['total_marks'],ascending = False)).reset_index(drop = True)
reportOfTopStudents=df.groupby('department').head(5)
print("Report of Top 5 Students in each Department")
print(reportOfTopStudents)


