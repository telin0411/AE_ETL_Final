#-*- coding: utf-8 -*-
#!/usr/bin/python
#----------------------------------------------------------------------------
# Name:         DB_Init.py
# Purpose:      Initialize the Database and all required collections for AE_ETL
# Author:       Albert1_Wu
# Version:      2.00a
# Created:      2015.04.20
# Licence:      Pegartoncorp Inc. license
#----------------------------------------------------------------------------
import sys
import time
import copy
import datetime
import happybase
import collections
import AEETLClass as ae
from pymongo import MongoClient
from pymongo import Connection
from pymongo import ASCENDING, DESCENDING
from copy import deepcopy
from bson.code import Code
from parameter import (
    mongoIP, mongoPort, hbaseIP, check_switch, HBaseTable_Downtime, 
    HBaseTable_Yield, Mongo_N71_DB, MongoCheck_Collection, MongoSubStationName_Collection,
    MongoStationIP_Collection, startTime, stopsTime, BySubStationName_Shifts, 
    ByStationIP_Shifts, BySubStationName_Rank, ByStationIP_Rank,LastResult, LastQueryTime,
    StationSummary, ByStation, ByStation_Shifts)
    
def createDatabase(name):
    connection = Connection(mongoIP, mongoPort)
    db = connection[name]
    collection = db['testCollection']
    collection.insert({"1": 1})
    collection.remove({"1": 1})
    collection.drop()

def createBySubStationName(db):
    schema =  {
                  'downtime': 0,
                  'line': "0",
                  'downtime_error': 0,
                  'time': "0", 
                  'date': "0",
                  'station_name': "0",
                  'station_ip': "0",
                  'sub_station_name': "0"
              }
    args = ["date", "time", "station_ip", "sub_station_name"]
    db.createIndex(MongoSubStationName_Collection, args)
    db.insert(MongoSubStationName_Collection, schema)
    db.remove(MongoSubStationName_Collection, schema)
        
def createByStationIP(db):
    schema =  {
                  'downtime': 0,
                  'line': "0",
                  'downtime_error': 0,
                  'time': "0", 
                  'date': "0",
                  'station_name': "0",
                  'station_ip': "0",
              }
    args = ["date", "time", "station_ip"]
    db.createIndex(MongoStationIP_Collection, args)
    db.insert(MongoStationIP_Collection, schema)
    db.remove(MongoStationIP_Collection, schema)
    
def createBySubStationName_Shifts(db):
    schema =  {
                  'downtime': 0,
                  'line': "0",
                  'downtime_error': 0,
                  'shift': "0", 
                  'date': "0",
                  'station_name': "0",
                  'station_ip': "0",
                  'stations': "0"
              }
    args = ["date", "shift", "station_ip", "sub_station_name"]
    db.createIndex(BySubStationName_Shifts, args)
    db.insert(BySubStationName_Shifts, schema)
    db.remove(BySubStationName_Shifts, schema)
    
def createByStationIP_Shifts(db):
    schema =  {
                  'downtime': 0,
                  'line': "0",
                  'downtime_error': 0,
                  'shift': "0", 
                  'date': "0",
                  'station_name': "0",
                  'station_ip': "0",
              }
    args = ["date", "shift", "station_ip"]
    db.createIndex(ByStationIP_Shifts, args)
    db.insert(ByStationIP_Shifts, schema)
    db.remove(ByStationIP_Shifts, schema)
    
def createBySubStationName_Rank(db):
    schema =  {
                  'station_ip': "0",
                  'rank': "0"
              }
    args = ["station_ip"]
    db.createIndex(BySubStationName_Rank, args)
    db.insert(BySubStationName_Rank, schema)
    db.remove(BySubStationName_Rank, schema)
    
def createByStationIP_Rank(db):
    schema =  {
                  'station_name': "0",
                  'rank': "0",
              }
    args = ["station_name"]
    db.createIndex(ByStationIP_Rank, args)
    db.insert(ByStationIP_Rank, schema)
    db.remove(ByStationIP_Rank, schema)

def createCheck(db):
    schema =  {
                  'countY': "0",
                  'countD': "0",
                  'datetime': "0"
              }
    args = ["datetime"]
    db.createIndex(MongoCheck_Collection, args)
    db.insert(MongoCheck_Collection, schema)
    db.remove(MongoCheck_Collection, schema)    

def createLastMode(db):
    schema =  {
                  'lastCode': "0",
                  'lastMode': "0", 
                  'lastTime': "0",
                  'station_ip_sub': "0",
              }
    args = ['station_ip_sub']
    db.createIndex(LastResult, args)
    db.insert(LastResult, schema)
    db.remove(LastResult, schema)

def createLastTime(db):
    schema =  {
                  'LastQueryTime': "0",
                  'LastInsertTime': "0"
              }
    args = ['LastQueryTime']
    db.createIndex(LastQueryTime, args)
    db.insert(LastQueryTime, schema)
    db.remove(LastQueryTime, schema)

def createSummary(db):
    schema =  {
                  'update_time': "0",
                  'station_name': "0",
                  'line': "0"
              }
    args = ['station_name', 'line']
    db.createIndex(StationSummary, args)
    db.insert(StationSummary, schema)
    db.remove(StationSummary, schema)

def createByStation(db):
    schema =  {
                  'downtime': 0,
                  'line': "0",
                  'time': "0", 
                  'date': "0",
                  'station_name': "0"
              }
    args = ["date", "time", "station_name", "line"]
    db.createIndex(ByStation, args)
    db.insert(ByStation, schema)
    db.remove(ByStation, schema)

def createByStation_Shifts(db):
    schema =  {
                  'downtime': 0,
                  'line': "0",
                  'shift': "0", 
                  'date': "0",
                  'station_name': "0"
              }
    args = ["date", "shift", "station_name", "line"]
    db.createIndex(ByStation_Shifts, args)
    db.insert(ByStation_Shifts, schema)
    db.remove(ByStation_Shifts, schema)

def DropWhole():
    db.drop(MongoSubStationName_Collection)
    db.drop(MongoStationIP_Collection)
    db.drop(BySubStationName_Shifts)
    db.drop(ByStationIP_Shifts)
    db.drop(BySubStationName_Rank)
    db.drop(ByStationIP_Rank)
    db.drop(MongoCheck_Collection)
    db.drop(StationSummary)    
    db.drop(ByStation) 
    db.drop(ByStation_Shifts)    
    db.drop(LastResult) 
    
# Main Function
if __name__ == "__main__":
    name = Mongo_N71_DB
    #createDatabase(name)
    db = ae.mongo(name, mongoIP, mongoPort)
    if len(sys.argv) < 2:
        create = 1
    else:
        create = int(sys.argv[1])
    if create:
        print "Initialize all collections!"
        createDatabase(Mongo_N71_DB)
        createBySubStationName(db)
        createByStationIP(db)
        createBySubStationName_Shifts(db)
        createByStationIP_Shifts(db)
        createCheck(db)
        createLastMode(db)
        createLastTime(db)
        createSummary(db)
        createByStation(db)
        createByStation_Shifts(db)
    else:
        print "Drop all collections!"
        DropWhole()