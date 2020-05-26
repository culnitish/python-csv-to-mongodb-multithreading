import pymongo
import sys
from pathlib import Path
import os
import pandas as pd
import json
from pprint import pprint
from myLogger import myLog
import time
import threading
log = myLog(__name__)
from dbConnection import myCollection
from os import listdir
from os.path import isfile, join
import logging

# from dotenv import load_dotenv
# load_dotenv()

# # OR, the same with increased verbosity
# load_dotenv(verbose=True)

# # OR, explicitly providing path to '.env'
# # python3 only
# env_path = Path('.') / '.env'
# load_dotenv(dotenv_path=env_path)

filesAdded = dict()

filepath = '/home/user/nitish/personal_projects/python-csv-to-mongodb-multithreading/src/DataGeneration/StudentData/'  # pass csv file path


def import_content(filepath, index):
    # cdir = os.path.dirname(__file__)
    # file_res = os.path.join(cdir, filepath)
    print(filepath)
    data = pd.read_csv(filepath)
    logging.info('Thread {} is executing : '.format(index))
    data_json = json.loads(data.to_json(orient='records'))
    # myCollection.remove({})
    myCollection.insert_many(data_json)


def csvToDatabase(filepath):
    num = 3
    while (num):
        filesFetched = [
            files for files in listdir(filepath)
            if isfile(join(filepath, files))
        ]
        threads = list()

        for i in range(len(filesFetched)):
            logging.info("csvToDatabase : create and start thread %d.", i)

            fileName = filesFetched[i].split('.')[0]
            if fileName not in filesAdded:
                x = threading.Thread(target=import_content,
                                     args=(
                                         filepath + filesFetched[i],
                                         i,
                                     ))
                threads.append(x)
                logging.info('Thread {} is starting'.format(i))
                x.start()
                # import_content(filepath + filesFetched[i])
                filesAdded[fileName] = 1
                for index, thread in enumerate(threads):
                    logging.info("csvToDatabase : before joining thread %d.",
                                 index)
                    thread.join()
                    logging.info("csvToDatabase : thread %d done", index)
                    logging.info('{} is added in DB by thread {}'.format(
                        fileName, i))
        num -= 1


if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
    start = time.time()
    csvToDatabase(filepath)
    end = time.time()
    print(end - start)
