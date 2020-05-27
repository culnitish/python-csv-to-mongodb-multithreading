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
logging = myLog(__name__)
from dbConnection import myCollection
from os import listdir
from os.path import isfile, join
import concurrent.futures
THREAD_COUNT = 3
# import logging

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


def importContent(filepath, index):
    # cdir = os.path.dirname(__file__)
    # file_res = os.path.join(cdir, filepath)
    print(filepath)
    data = pd.read_csv(filepath)
    logging.info('Thread {} is executing : '.format(index))
    data_json = json.loads(data.to_json(orient='records'))
    # myCollection.remove({})
    myCollection.insert_many(data_json)

def threadCreation(filepath,index):
    logging.info("threadCreation : create and start thread %d.", index)
    createdThread = threading.Thread(target=importContent,args=(filepath,index,))
    logging.info('Thread {} is starting'.format(index))
    createdThread.start()
    return createdThread

def threadJoin(threads):
    for index, thread in enumerate(threads):
        logging.info("csvToDatabase : before joining thread %d.",index)
        thread.join()
        logging.info("csvToDatabase : thread %d done", index)
        # logging.info('{} is added in DB by thread {}'.format(fileName, i))   
def csvToDatabase(filepath,desiredThreadCount):
    num = 1
    while (num):
        try:
            filesFetched = [files for files in listdir(filepath) if isfile(join(filepath, files))]
            threadsArr = list()
            with concurrent.futures.ThreadPoolExecutor(max_workers=desiredThreadCount) as executor:
                for i in range(len(filesFetched)):
                    fileName = filesFetched[i].split('.')[0]
                    if fileName not in filesAdded:
                        # logging.info("No of active thread Count:{}".format(threading.active_count()))
                        executor.submit(importContent,filepath + filesFetched[i],i)
                        # currentThread = threadCreation(filepath + filesFetched[i],i)
                        # threadsArr.append(currentThread)
                        # importContent(filepath + filesFetched[i])
                        filesAdded[fileName] = 1
                        # if threading.active_count() == desiredThreadCount:  # set maximum threads.
                        #     threadJoin(threadsArr)
                        
                    # logging.info("No of active thread Count:{}".format(threading.active_count()))
                    
                    
                num -= 1
        except Exception as e:
            logging.info("Error: unable to start thread {}".format(e))    


if __name__ == "__main__":
    # format = "%(asctime)s: %(message)s"
    # logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
    start = time.time()
    csvToDatabase(filepath,THREAD_COUNT)
    end = time.time()
    print(end - start)
        