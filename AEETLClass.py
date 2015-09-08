import sys
import happybase
import pymongo
from bson import json_util, ObjectId
import bson

class mongo:
    def __init__(self, database, ip="172.28.138.62", port=27017):
        self.connection = pymongo.MongoClient(ip, port)
        self.db = self.connection[database]
        try:
            self.db.authenticate("hduser", "pega#1234")
        except Exception as e:
            print "### Authentication failed! But seems doesn't matter... (?)"
            print e.args
        self.collection = None
        self.cursor = None

    def insert(self, collection, data):
        self.collection = self.db[collection]
        try:
            self.collection.insert(data)
        except bson.errors.InvalidStringData as e:
            print "-------------------------------------------------------------------"
            print "Insertion Failed:", data 
            print e.message
            print "-------------------------------------------------------------------"
            #print "**************************************************************"
            #print "**** Data Below are duplicated, and thus dropped directly ****"
            #print "**************************************************************"
            #for each in data:
            #    print each

    def find(self, collection, condition):
        self.collection = self.db[collection]
        retDict = []
        search = self.collection.find(condition)
        if search.count() > 0:
            for document in search:
                #print json_util.dumps(document)
                #print document
                #retDict.append(json_util.dumps(document))
                retDict.append(document)
            #print "Find", search.count(), "documents"
        else:
            pass
            #print "No such file"
        return retDict

    def set_unique_key(self, collection, key):
        db = self.db[collection]
        if type(key) == list:
            key_dict = []
            for i in range(0, len(key)):
                key_dict.append((key[i], 1))
            db.ensure_index(key_dict, unique = True)
        else:
            db.ensure_index(key, unique = True)

    def update(self, collection, query, data):
        db = self.db[collection]
        #print "Update", update_result["nModified"], "documents"
        try:
            update_result = db.update(query, {"$set": data})
        except bson.errors.InvalidStringData as e:
            print "-------------------------------------------------------------------"
            print "Insertion Failed:", data 
            print e.message
            print "-------------------------------------------------------------------"

    def remove(self, collection, condition):
        self.collection = self.db[collection]
        self.collection.remove(condition)
    
    def drop(self, collection):
        self.db[collection].drop()
    
    def createCollection(self, collection):
        self.collection = self.db[collection] 
    
    def createIndex(self, collection, args):
        tmp = []
        for e in args:
            tmp.append((e, pymongo.ASCENDING))
        self.db[collection].create_index(tmp, unique=True, dropDups=True)
    
    def close(self):
        self.connection.close()
    
    def MapReduce(self, collection, mapper, reducer, out="myresults"):
        db = self.db[collection]
        result = db.map_reduce(mapper, reducer, out)
        return result

class hbase:
    def __init__(self, table, ip="172.18.212.128"):
        self.connection = happybase.Connection(host=ip)
        self.connection.open()
        self.table = self.connection.table(table)
        self.start = None
        self.stop  = None
        print "HBase Connected"
        
    def Scan(self, rowStart=None, rowStop=None):
        retDict = {}
        if rowStart != None:
            self.start = rowStart
        if rowStop != None:
            self.stop = rowStop
        for key, data in self.table.scan(row_start=self.start, row_stop=self.stop):
            retDict[key] = data
        return retDict

    def ScanReturnList(self, rowStart=None, rowStop=None):
        retList = []
        if rowStart != None:
            self.start = rowStart
        if rowStop != None:
            self.stop = rowStop
        for key, data in self.table.scan(row_start=self.start, row_stop=self.stop):
            data["key"] = key
            retList.append(data)
        return retList

    def ScanReturnList_InserTime(self, rowStart=None, rowStop=None):
        cont = 0
        retList = []
        tmpList = []
        lastTime = ""
        if rowStart != None:
            self.start = rowStart
        if rowStop != None:
            self.stop = rowStop
        for key, data in self.table.scan(row_start=self.start, row_stop=self.stop):
            keys = key.split("_")
            insertTime = keys[0]
            eventsTime = keys[1]
            newKey     = "_".join(keys[1:])
            data["key"] = newKey
            ###
            #cont = 0
            #dat = data['cf1:col1']
            #dats = dat.split(",")
            #for each in dats:
            #    line = each.split(":")
            #    if line[0] == "DateTime" and "/" in line[1]:
            #        cont = 1
            #if cont == 1:
            #    continue
            ###
            tmp = [eventsTime, data]
            tmpList.append(tmp)
            lastTime = insertTime
        tmpList = sorted(tmpList)
        for each in tmpList:
            retList.append(each[1])
        return retList, lastTime
   
    def close(self):
        self.connection.close()  