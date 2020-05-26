import pymongo
import sys
import os
import pandas as pd
import json
from pprint import pprint
from myLogger import myLog
from dbConnection import myCollection
log = myLog(__name__)
#DataBase Connection
# client = pymongo.MongoClient(
#     "mongodb+srv://root:root@cluster-aqz47.mongodb.net/test?retryWrites=true&w=majority"
# )
# myDatabase = client['personal_project']
# myCollection = myDatabase['students']

# myCollection.insert_one({
#     "registration_id": 15465,
#     "student_name": "Abhishek Kumar",
#     "date_of_birth": "08/01/2003",
#     "email": "abhishek@gmail.com",
#     "address": "Mathura",
#     "department": "Computer Science",
#     "sub1": 45,
#     "sub2": 67,
#     "sub3": 78,
#     "sub4": 67,
#     "sub5": 78
# })
# # print(myCollection)
# data = myCollection.find({"registration_id": 15265}).count_documents()
# cursor = myCollection.find({})
# # print(cursor)
# for document in cursor:
#     pprint(document)
#     log.info(document)


def import_content(filepath):
    # cdir = os.path.dirname(__file__)
    # file_res = os.path.join(cdir, filepath)

    data = pd.read_csv(filepath)
    data_json = json.loads(data.to_json(orient='records'))
    # myCollection.()
    myCollection.insert_many(data_json)


if __name__ == "__main__":
    filepath = '/home/user/nitish/personal_projects/python-csv-to-mongodb-multithreading/src/DataGeneration/student.csv'  # pass csv file path
    import_content(filepath)
