import pymongo
client = pymongo.MongoClient(
    "mongodb+srv://root:root@cluster-aqz47.mongodb.net/test?retryWrites=true&w=majority"
)
myDatabase = client['personal_project']
myCollection = myDatabase['students']