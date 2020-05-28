# python-csv-to-mongodb-multithreading
The Python Script which will be running for infinite time and will take a multiple csv file from path and dump the data in mongoDB , using multi-threading in python

# To run the Program first Clone the Project and Open the Project Folder "python-csv-to-mongodb-multithreading"
   1. ->cd src
   2.See the list of Programs index.py is the main program start
   3.Then execute for execution of code
   4.->python3 index.py 
   5.After exection this takes input from src/DataGeneration/StudentData and dump the data in mongo DataBase
 # config.ini files consist of
    1.FILE_LOC = ./DataGeneration/StudentData/ #Specifies the directory of CSV Files
    2.THREAD_COUNT = 3 #No of thread Counts
 
 # To Generate the test Data , Faker library is used
   1.->cd src/DataGeneration
   2.To execute the fakedata thing
   3.->python3 fakedata.py
   4.After execution the csv Files got generated in src/DataGeneration/StudentData Directory
# config.ini file in Project contains the Default values
    1.RECORD_COUNT = 10000 #that maintains the number of records to generate per csv File
    2.NO_OF_CSV_FILES =2  #that maintains number of fake csv files to generate 

  
  
 
