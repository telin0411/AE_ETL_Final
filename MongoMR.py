import sys
import time
import datetime
import happybase
import AEETLClass as ae
from pymongo import MongoClient
from copy import deepcopy
from bson.code import Code
from parameter import (
    mongoIP, mongoPort, hbaseIP, check_switch, HBaseTable_Downtime, 
    HBaseTable_Yield, Mongo_N71_DB, MongoCheck_Collection, MongoSubStationName_Collection,
    MongoStationIP_Collection, startTime, stopsTime, BySubStationName_Shifts, 
    ByStationIP_Shifts, BySubStationName_Rank, ByStationIP_Rank, LastResult, DayShift, NightShift,
    StationSummary, ByStation, ByStation_Shifts)

# Global variables for MongoDB
mongoIP = '172.28.138.62'
mongoPort = 27017

def timeFormat(t1):
    yy1 = int(t1[0:4])
    mm1 = int(t1[4:6])
    dd1 = int(t1[6:8])        
    hh1 = int(t1[8:10])
    mn1 = int(t1[10:12])
    ss1 = int(t1[12:14])
    tt1 = datetime.datetime(yy1, mm1, dd1, hh1, mn1, ss1)   
    return tt1
    
def timeDiff(t1, t2):
    tt1 = timeFormat(t1)
    tt2 = timeFormat(t2)
    diff = tt2 - tt1
    diffDays = diff.days
    diffSecs = diff.seconds
    total = diff.days * 86400 + diffSecs
    return total
    
def timeHrAdd(t1, hrs):
    tt1 = timeFormat(t1)
    tt = tt1 + datetime.timedelta(hours=hrs)
    out = str(tt)
    outstr = out[0:4] + out[5:7] + out[8:10] + out[11:13] + out[14:16] + out[17:19]
    return outstr

def Aggregate(db, startTime, stopsTime, shiftTime, shift):
    subs = {}
    ips = {}
    stations = {}
    for i in range(-shiftTime):
        dt = timeHrAdd(startTime, i)
        resSub = db.find(MongoSubStationName_Collection, {"date":dt[0:8], "time":dt[8:14]})
        for each in resSub:
            ipsub = each["station_ip"] + "_" + each["sub_station_name"]
            if ipsub not in subs:
                subs[ipsub] = each
                subs[ipsub]['date'] = startTime[0:8]
                del subs[ipsub]["time"]
                subs[ipsub]["shift"] = shift
            else:
                for code in each["downtime_error"]:
                    if code not in subs[ipsub]["downtime_error"]:
                        subs[ipsub]["downtime_error"][code] = each["downtime_error"][code]
                    else:
                        subs[ipsub]["downtime_error"][code]["count"] += each["downtime_error"][code]["count"]
                        subs[ipsub]["downtime_error"][code]["duration"] += each["downtime_error"][code]["duration"]
                subs[ipsub]["downtime"] += each["downtime"]
        resIP = db.find(MongoStationIP_Collection, {"date":dt[0:8], "time":dt[8:14]})
        for each in resIP:
            ip = each["station_ip"]
            if ip not in ips:
                ips[ip] = each
                ips[ip]['date'] = startTime[0:8]
                del ips[ip]["time"]
                ips[ip]["shift"] = shift                
            else:
                for code in each["downtime_error"]:
                    if code not in ips[ip]["downtime_error"]:
                        ips[ip]["downtime_error"][code] = each["downtime_error"][code]
                    else:
                        ips[ip]["downtime_error"][code]["count"] += each["downtime_error"][code]["count"]
                        ips[ip]["downtime_error"][code]["duration"] += each["downtime_error"][code]["duration"]
                ips[ip]["downtime"] += each["downtime"]
        resSt = db.find(ByStation, {"date":dt[0:8], "time":dt[8:14]})
        for each in resSt:
            station = each["line"] + "_" + each["station_name"]
            if station not in stations:
                stations[station] = each
                stations[station]['date'] = startTime[0:8]
                del stations[station]["time"]
                stations[station]["shift"] = shift
            else:
                stations[station]["downtime"] += each["downtime"]
            #if each["station_name"] == "BBS":
            #    print each
            #    print each["downtime"]
            #    print stations[station]["downtime"]
    return subs, ips, stations

def MRmain(db, startTime, stopsTime, subCol, ipCol, shiftTime, shift):
    subs, ips, stations = Aggregate(db, startTime, stopsTime, shiftTime, shift)
    return subs, ips, stations
    
## Test
if __name__ == "__main__":
    table = "test"
    db = ae.mongo(table, mongoIP, mongoPort)    
    startTime = "20150403140000"
    stopsTime = "20150422140000"
    s = MongoSubStationName_Collection
    i = MongoStationIP_Collection
    MRmain(db, startTime, stopsTime, s, i)