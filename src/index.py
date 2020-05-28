#Database Setup
import pymongo
from dbConnection import myCollection
import sys
from pathlib import Path
import os
import pandas as pd
import json
#Logging Setup
from myLogger import myLog
logging = myLog(__name__)
import time
#Thread Setup
import threading
import concurrent.futures
from os import listdir
from os.path import isfile, join
#Config File Setup
from configparser import ConfigParser
config = ConfigParser()
config.read('../config.ini')

filesAdded = dict() #Global File for Checking of files Added
'''Decorator is used for time logging of a method'''
def time_it(method):
    def wrapper(name_ref,s):
        start = time.time()
        result = method(name_ref,s)
        end = time.time()
        print(method.__name__+" method time taken {} milliSeconds".format((end-start)*1000))
        return result
    return wrapper

def csvToDatabase(filepath):
    print(filepath)
    data = pd.read_csv(filepath)
    data_json = json.loads(data.to_json(orient='records'))
    # myCollection.remove({})
    myCollection.insert_many(data_json)
    logging.info('{} Filename is added in Database'.format(filepath))

@time_it
def threadGeneration(filepath,desiredThreadCount):
    num = 1
    while (num):
        try:
            filesFetched = [files for files in listdir(filepath) if isfile(join(filepath, files))]
            print(filesFetched)
            threadsArr = list()
            with concurrent.futures.ThreadPoolExecutor(max_workers=desiredThreadCount) as executor:
                for i in range(len(filesFetched)):
                    fileName = filesFetched[i].split('.')[0]
                    if fileName not in filesAdded:
                        logging.info('New Thread is created')
                        executor.submit(csvToDatabase,filepath + filesFetched[i])
                        filesAdded[fileName] = 1
                    
            num -= 1
        except Exception as e:
            logging.info("Error: unable to start thread {}".format(e))    


if __name__ == "__main__":
    '''Config file Retirieved'''
    threadCount = int(config['THREADINFO']['THREAD_COUNT'])
    filepath = config['FILEINFO']['FILE_LOC']
    '''Function to create Threads'''
    threadGeneration(filepath,threadCount)