import pymongo
import sys
from pathlib import Path
import os
import pandas as pd
import json
from pprint import pprint
from myLogger import myLog
from dbConnection import myCollection
log = myLog(__name__)
from os import listdir
from os.path import isfile, join

# from dotenv import load_dotenv
# load_dotenv()

# # OR, the same with increased verbosity
# load_dotenv(verbose=True)

# # OR, explicitly providing path to '.env'
# # python3 only
# env_path = Path('.') / '.env'
# load_dotenv(dotenv_path=env_path)

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
filesAdded = dict()

filepath = '/home/user/nitish/personal_projects/python-csv-to-mongodb-multithreading/src/DataGeneration/StudentData/'  # pass csv file path


def csvToDatabase(filepath):
    while (1):
        filesFetched = [
            files for files in listdir(filepath)
            if isfile(join(filepath, files))
        ]
        for i in range(len(filesFetched)):
            fileName = filesFetched[i].split('.')[0]
            if fileName not in filesAdded:
                import_content(filepath + filesFetched[i])
                filesAdded[fileName] = 1
                log.info('{} is added in DB'.format(fileName))


def import_content(filepath):
    # cdir = os.path.dirname(__file__)
    # file_res = os.path.join(cdir, filepath)
    print(filepath)
    data = pd.read_csv(filepath)
    data_json = json.loads(data.to_json(orient='records'))
    # myCollection.()
    myCollection.insert_many(data_json)


if __name__ == "__main__":
    csvToDatabase(filepath)
    # import_content(filepath)
