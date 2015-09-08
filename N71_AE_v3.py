#-*- coding: utf-8 -*-
#!/usr/bin/python
#------------------------------------------------------------------------------------------
# Name:         N71_AE_v3.py
# Purpose:      ETL from HBase to MongoDB, AE related, use HBase self-contained timestamps
# Author:       Albert1_Wu
# Version:      4.00a
# Created:      2015.05.08
# Licence:      Pegartoncorp Inc. license
#------------------------------------------------------------------------------------------
import sys
import time
import copy
import datetime
import happybase
import collections
import AEETLClass as ae
from pymongo import MongoClient
from copy import deepcopy
from bson.code import Code
from MongoMR import MRmain
from Downtime import aggregateDowntime
from Downtime import StationSummarize
from parameter import (
    mongoIP, mongoPort, hbaseIP, check_switch, HBaseTable_Downtime, 
    HBaseTable_Yield, Mongo_N71_DB, MongoCheck_Collection, MongoSubStationName_Collection,
    MongoStationIP_Collection, startTime, stopsTime, BySubStationName_Shifts, 
    ByStationIP_Shifts, BySubStationName_Rank, ByStationIP_Rank, LastResult, DayShift, NightShift,
    LastQueryTime, StationSummary, ByStation_Shifts, ByStation)

# Source: http://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance#Python
def levenshtein(s1, s2):
    if len(s1) < len(s2):
        return levenshtein(s2, s1)
    if len(s2) == 0:
        return len(s1) 
    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1 
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row 
    return previous_row[-1]

#####
def extractStationName(ip):
    name = ip.split("_")
    return name[-3]

def extractStationLine(ip):
    name = ip.split("_")
    lineName = name[2]
    line = lineName[2] + "F" + lineName[-2:len(lineName)]
    return line

def extractSubStation(ip):
    name = ip.split("_")
    return name[1] + "_" + name[2] + "_" + name[3] + "_" + name[4] + "_" + name[5]

def extractIP(ip):
    name = ip.split("_")
    return name[1] + "_" + name[2] + "_" + name[3]
#####

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

def timeSecAdd(t1, sec):
    tt1 = timeFormat(t1)
    tt = tt1 + datetime.timedelta(seconds=sec)
    out = str(tt)
    outstr = out[0:4] + out[5:7] + out[8:10] + out[11:13] + out[14:16] + out[17:19]
    return outstr    

# The main ETL function
def ETL_Downtime(resD, db):
    ## List for standard keys
    Standard_List = ["time mode", "err code", "datetime", "ae no.", "hw rev.", "sw rev.", "description", "color"]
    station_sub = collections.defaultdict(dict)
    station_sub_db = collections.defaultdict(str) # For storing all info in case needed
    retIP = collections.defaultdict(dict)
    #retIP, retSt = aggregateDowntime(resD, db)    # For processing the downtime to station level
    for dicts in resD:
        stationSub = extractSubStation(dicts['key'])
        tmp = collections.defaultdict(str)
        info = dicts['cf1:col1'].split(",")
        for items in info:
            ele = items.split(":")
            try:
                tmp[ele[0].lower()] = ele[1]
            except:
                print "WTF is Vendor doing!", ele
        ### Levenshterin checking the min edit distance between input dict keys and standard keys
        ### ie. "error code" -> "err code"        
        todel = []
        for stlist in Standard_List:
            helper = {}
            for dict_keys in tmp:
                helper[levenshtein(dict_keys, stlist)] = dict_keys
            closest = min(helper)
            todel.append((helper[closest], stlist))
        for ele in todel:
            tmpinfo = tmp[ele[0]]
            del tmp[ele[0]]
            tmp[ele[1]] = tmpinfo.strip()
        ### end of Levenshtein tuning
        station_sub[stationSub]["ae_no"] = tmp["ae no."]
        station_sub[stationSub]["hw_version"] = tmp["hw rev."]
        station_sub[stationSub]["sw_version"] = tmp["sw rev."]
        #tmp["description"] = tmp["description"].encode('utf8')
        #print "Color =", tmp["color code"]
        #print "Des =", tmp["description"]
        if "downtime_error" not in station_sub[stationSub]:
            station_sub[stationSub]["downtime_error"] = collections.defaultdict(dict)
            station_sub[stationSub]["downtime_error"]['N/A'] = {'count': 0}
            station_sub[stationSub]["downtime_error"]["lastRec"] = []
            
        if tmp['err code'] not in station_sub[stationSub]["downtime_error"]:
            if tmp['time mode'] == '0':
                station_sub[stationSub]["downtime_error"]["lastRec"].append((tmp['datetime'], tmp['time mode'], tmp['err code'], tmp["color"], tmp["description"]))
                station_sub[stationSub]["downtime_error"][tmp['err code']] = {'count'      : 0,
                                                                              'duration'   : 0,
                                                                              'color'      : tmp["color"],
                                                                              'description': tmp["description"]}
                station_sub[stationSub]["downtime_error"]['N/A']['count'] = 1
                db.remove(LastResult, {"station_ip_sub": stationSub})
        if station_sub[stationSub]["downtime_error"]['N/A']['count'] == 0:
            if tmp['time mode'] == '1':
                station_sub[stationSub]["downtime_error"]['N/A']['count'] = 1
                lastResult = db.find(LastResult, {"station_ip_sub": stationSub})
                db.remove(LastResult, {"station_ip_sub": stationSub})
                if lastResult:
                    lr = lastResult[0]
                    if lr['lastTime'][8:10] == tmp['datetime'][8:10]:
                        station_sub[stationSub]["downtime_error"]["lastRec"].append((lr['lastTime'], lr['lastMode'], lr['lastCode'], lr["lastColor"], lr["lastDes"]))
                        station_sub[stationSub]["downtime_error"][lr['lastCode']] = {'count'      : 0,
                                                                                     'duration'   : 0,
                                                                                     'color'      : lr["lastColor"],
                                                                                     'description': lr["lastDes"]}
                    else:
                        def getInfo(ip):
                            details = ip.split("_")
                            name = details[3]
                            lines = details[1]
                            line = lines[2] + "F" + lines.split("-")[2][0:2]
                            ip = "_".join(details[0:4])
                            sub = details[-1]
                            return name, line, ip, sub
                        lastNextHour = timeHrAdd(lr['lastTime'], 1)[0:10] + "0000"
                        lastDuration = timeDiff(lr['lastTime'], lastNextHour)
                        modDate = lr['lastTime'][0:8]
                        modTime = lr['lastTime'][8:10] + "0000"
                        modIP  = "_".join(stationSub.split("_")[:-1])
                        modSub = stationSub.split("_")[-1]
                        modStat = stationSub.split("_")[-2]
                        modLines = stationSub.split("_")[1]
                        modLine = modLines[2] + "F" + modLines.split("-")[2][0:2]
                        lastQuery = {"station_ip":modIP, 
                                     "sub_station_name":modSub, 
                                     "date":modDate, 
                                     "time":modTime}
                        if not db.find(MongoSubStationName_Collection, lastQuery):
                            continue
                        toModify = db.find(MongoSubStationName_Collection, lastQuery)[0]
                        toModify["downtime"] += int(lastDuration)
                        toModify["downtime_error"][lr['lastCode']]["duration"] += int(lastDuration)
                        db.update(MongoSubStationName_Collection, lastQuery, toModify)
                        #print lastQuery
                        #print toModify 
                        lastQueryIP = {"station_ip":modIP, 
                                       "date":modDate, 
                                       "time":modTime} 
                        if not db.find(MongoStationIP_Collection, lastQueryIP):
                            continue
                        toModifyIP = db.find(MongoStationIP_Collection, lastQueryIP)[0]
                        toModifyIP["downtime"] += int(lastDuration)
                        toModifyIP["downtime_error"][lr['lastCode']]["duration"] += int(lastDuration)
                        db.update(MongoStationIP_Collection, lastQueryIP, toModifyIP)
                        #print lastQueryIP
                        #print toModifyIP
                        lastQueryStat = {"station_name":modStat,
                                         "line":modLine, 
                                         "date":modDate, 
                                         "time":modTime}
                        #print lastQueryStat
                        lastQueryIPStat = {"station_name":modStat,
                                           "line":modLine, 
                                           "date":modDate, 
                                           "time":modTime}
                        ipNum = len(db.find(MongoStationIP_Collection, lastQueryIPStat))
                        if not db.find(ByStation, lastQueryStat):
                            continue
                        toModifyStat = db.find(ByStation, lastQueryStat)[0]
                        toModifyStat["downtime"] += int(lastDuration) / ipNum
                        db.update(ByStation, lastQueryStat, toModifyStat)
                        #print lastQueryStat
                        #print toModifyStat
                        gapHours = timeDiff(lastNextHour, tmp['datetime'][0:10] + "0000") / 3600
                        middleTimes = lastNextHour                        
                        for i in range(gapHours):
                            print "# New insertion at", middleTimes, "due to historical value!", stationSub
                            ### Subs
                            middleDicts = collections.defaultdict(dict)
                            tmpDE = {lr['lastCode']: {'count'       : 0,
                                                      'duration'    : 3600,
                                                      'color'       : toModify["downtime_error"][lr['lastCode']]["color"],
                                                      'description' : toModify["downtime_error"][lr['lastCode']]["description"]}
                                    }
                            station_name, line, station_ip, sub_station_name = getInfo(stationSub)  
                            middleDicts["date"] = middleTimes[0:8]
                            middleDicts["time"] = middleTimes[8:14]    
                            middleDicts["station_name"] = station_name
                            middleDicts["line"] = line
                            middleDicts["station_ip"] = station_ip
                            middleDicts["sub_station_name"] = sub_station_name
                            middleDicts["ae_no"] = station_sub[stationSub]["ae_no"]
                            middleDicts["sw_version"] = station_sub[stationSub]["sw_version"]
                            middleDicts["hw_version"] = station_sub[stationSub]["hw_version"] 
                            queryMid = {"date": middleTimes[0:8],
                                        "time": middleTimes[8:14],
                                        "sub_station_name": sub_station_name,
                                        "station_ip": station_ip}
                            resMid = db.find(MongoSubStationName_Collection, queryMid)
                            if resMid:
                                modifiedMid = resMid[0]
                                if lr['lastCode'] in modifiedMid["downtime_error"]:
                                    modifiedMid["downtime_error"][lr['lastCode']]["duration"] = 3600
                                    modifiedMid["downtime_error"][lr['lastCode']]["count"] += 0
                                else:
                                    modifiedMid["downtime_error"][lr['lastCode']] = {}
                                    modifiedMid["downtime_error"][lr['lastCode']]["duration"] = 3600 
                                    modifiedMid["downtime_error"][lr['lastCode']]["count"] = 0
                                modifiedMid["downtime"] += 3600
                                db.update(MongoSubStationName_Collection, queryMid, modifiedMid) 
                            else:
                                middleDicts["downtime_error"] = tmpDE
                                middleDicts["downtime"] = 3600
                                db.insert(MongoSubStationName_Collection, middleDicts)      
                            ### IPs
                            middleDicts = collections.defaultdict(dict)
                            middleDicts["date"] = middleTimes[0:8]
                            middleDicts["time"] = middleTimes[8:14]    
                            middleDicts["station_name"] = station_name
                            middleDicts["line"] = line
                            middleDicts["station_ip"] = station_ip
                            queryMid = {"date": middleTimes[0:8],
                                        "time": middleTimes[8:14],
                                        "station_ip": station_ip}
                            resMid = db.find(MongoStationIP_Collection, queryMid)
                            if resMid:
                                modifiedMid = resMid[0]
                                #print modifiedMid
                                if lr['lastCode'] in modifiedMid["downtime_error"]:
                                    modifiedMid["downtime_error"][lr['lastCode']]["duration"] = 3600
                                    modifiedMid["downtime_error"][lr['lastCode']]["count"] += 0
                                else:
                                    modifiedMid["downtime_error"][lr['lastCode']] = {}
                                    modifiedMid["downtime_error"][lr['lastCode']]["duration"] = 3600 
                                    modifiedMid["downtime_error"][lr['lastCode']]["count"] = 0
                                modifiedMid["downtime"] += 3600
                                db.update(MongoStationIP_Collection, queryMid, modifiedMid) 
                            else:
                                middleDicts["downtime_error"] = tmpDE
                                middleDicts["downtime"] = 3600
                                db.insert(MongoStationIP_Collection, middleDicts)
                            ### Stations
                            middleDicts = collections.defaultdict(dict)
                            middleDicts["date"] = middleTimes[0:8]
                            middleDicts["time"] = middleTimes[8:14]    
                            middleDicts["station_name"] = station_name
                            middleDicts["line"] = line
                            queryMid = {"date": middleTimes[0:8],
                                        "time": middleTimes[8:14],
                                        "station_name": station_name,
                                        "line": line}
                            resMid = db.find(ByStation, queryMid)
                            if resMid:
                                modifiedMid = resMid[0]
                                #modifiedMid["downtime"] += 3600
                                modifiedMid["downtime"] = 3600
                                db.update(ByStation, queryMid, modifiedMid) 
                            else:
                                middleDicts["downtime"] = 3600
                                db.insert(ByStation, middleDicts)
                            middleTimes = timeHrAdd(middleTimes, 1)                                
                        station_sub[stationSub]["downtime_error"]["lastRec"].append((middleTimes, lr['lastMode'], lr['lastCode']))
                        station_sub[stationSub]["downtime_error"][lr['lastCode']] = {'count'      : 0,
                                                                                     'duration'   : 0,
                                                                                     'color'      : lr["lastColor"],
                                                                                     'description': lr["lastDes"]}                    
                else:
                    print stationSub, "First Encountered Green but without any previous results"
                    continue
                    
        if not station_sub[stationSub]["downtime_error"]["lastRec"]:
            if tmp['time mode'] == '0':
                station_sub[stationSub]["downtime_error"]["lastRec"].append((tmp['datetime'], tmp['time mode'], tmp['err code'], tmp["color"], tmp["description"]))
            else:
                continue
        
        lastCode = station_sub[stationSub]["downtime_error"]["lastRec"][0][2]
        lastMode = station_sub[stationSub]["downtime_error"]["lastRec"][0][1]       
        thisTime = tmp['datetime']
        if lastMode == '0' and tmp['time mode'] == '1': # 0->1 count
            lastTime = station_sub[stationSub]["downtime_error"]["lastRec"][0][0]           
            diffTime = timeDiff(lastTime, thisTime)
            station_sub[stationSub]["downtime_error"][lastCode]['duration'] += diffTime
            station_sub[stationSub]["downtime_error"]["lastRec"] = []              
        if tmp['time mode'] == '0':   
            station_sub[stationSub]["downtime_error"][tmp['err code']]['count'] += 1
            station_sub[stationSub]["downtime_error"]["lastRec"].append((tmp['datetime'], tmp['time mode'], tmp['err code'], tmp["color"], tmp["description"]))
    return station_sub, retIP

# Aggregate to MongoDB capable data type
def AssembleTheResults(station_sub, startTime, retIP):
    ## Delete the helper items from the output dictionary
    ## And store the last value into MongoDB
    lastResults = {}
    for each in station_sub:
        lastResults[each] = {}
        if station_sub[each]["downtime_error"]["lastRec"]:
            lastResults[each]['lastCode'] = station_sub[each]["downtime_error"]["lastRec"][-1][2]
            lastResults[each]['lastTime'] = station_sub[each]["downtime_error"]["lastRec"][-1][0]
            lastResults[each]['lastMode'] = station_sub[each]["downtime_error"]["lastRec"][-1][1]
            lastResults[each]['lastColor'] = station_sub[each]["downtime_error"]["lastRec"][-1][3]
            lastResults[each]['lastDes'] = station_sub[each]["downtime_error"]["lastRec"][-1][4]
        del station_sub[each]["downtime_error"]["lastRec"]
        del station_sub[each]["downtime_error"]["N/A"]
    for each in station_sub:
        if not station_sub[each]["downtime_error"]:
            continue
        print "**********************************************************************"
        print each
        print station_sub[each]
    print "**********************************************************************"        
    date = startTime[0:8]
    time = startTime[8:14]
    Subs = collections.defaultdict(dict)
    ByIP = collections.defaultdict(dict)
    BySt = collections.defaultdict(dict)
    #### Extraction inline functions
    def getInfo(ip):
        details = ip.split("_")
        name = details[3]
        lines = details[1]
        line = lines[2] + "F" + lines.split("-")[2][0:2]
        ip = "_".join(details[0:4])
        sub = details[-1]
        return name, line, ip, sub
    #### End of the functions
    ## By Sub Station Names
    for key in station_sub: 
        if not station_sub[key]["downtime_error"]:
            continue
        station_name, line, station_ip, sub_station_name = getInfo(key)
        full_sub_name = "_".join([station_ip, sub_station_name])
        if full_sub_name not in Subs:
            Subs[full_sub_name] = {}
        Subs[full_sub_name]["date"] = date
        Subs[full_sub_name]["time"] = time    
        Subs[full_sub_name]["station_name"] = station_name
        Subs[full_sub_name]["line"] = line
        Subs[full_sub_name]["station_ip"] = station_ip
        Subs[full_sub_name]["sub_station_name"] = sub_station_name
        Subs[full_sub_name]["ae_no"] = station_sub[key]["ae_no"]
        Subs[full_sub_name]["sw_version"] = station_sub[key]["sw_version"]
        Subs[full_sub_name]["hw_version"] = station_sub[key]["hw_version"]
        downtime = 0
        for code in station_sub[key]["downtime_error"]:
            downtime += station_sub[key]["downtime_error"][code]["duration"]
        Subs[full_sub_name]["downtime"] = min(downtime, 3600)
        Subs[full_sub_name]["downtime_error"] = copy.deepcopy(station_sub[key]["downtime_error"])
    ## By IPs
        IP = station_ip
        if IP not in ByIP:
            ByIP[IP] = {}
            ByIP[IP]["date"] = date
            ByIP[IP]["time"] = time
            ByIP[IP]["station_name"] = station_name
            ByIP[IP]["line"] = line
            ByIP[IP]["station_ip"] = station_ip
            downtime = 0
            for code in station_sub[key]["downtime_error"]:
                downtime += station_sub[key]["downtime_error"][code]["duration"]
            ByIP[IP]["downtime"] = min(3600, downtime)
            ByIP[IP]["downtime_error"] = copy.deepcopy(station_sub[key]["downtime_error"])            
        else:
            downtime = ByIP[IP]["downtime"]
            for code in station_sub[key]["downtime_error"]:
                downtime += station_sub[key]["downtime_error"][code]["duration"]
                if code not in ByIP[IP]["downtime_error"]:
                    ByIP[IP]["downtime_error"][code] = station_sub[key]["downtime_error"][code]
                else:
                    ByIP[IP]["downtime_error"][code]["count"] += station_sub[key]["downtime_error"][code]["count"]
                    ByIP[IP]["downtime_error"][code]["duration"] += station_sub[key]["downtime_error"][code]["duration"]
            ByIP[IP]["downtime"] = min(3600, downtime)
    ## By Stats
        stat = line+"_"+station_name
        if stat not in BySt:
            BySt[stat] = {}
            BySt[stat]["date"] = date
            BySt[stat]["time"] = time
            BySt[stat]["station_name"] = station_name
            BySt[stat]["line"] = line
            downtime = 0
            for code in station_sub[key]["downtime_error"]:
                downtime += station_sub[key]["downtime_error"][code]["duration"]
            BySt[stat]['downtime'] = {}
            BySt[stat]['downtime'][IP] = min(3600, downtime)
        else:
            downtime = 0
            for code in station_sub[key]["downtime_error"]:
                downtime += station_sub[key]["downtime_error"][code]["duration"]            
            if IP in BySt[stat]['downtime']:
                BySt[stat]['downtime'][IP] += downtime
                BySt[stat]['downtime'][IP] = min(3600, downtime)
            else:
                BySt[stat]['downtime'][IP] = min(3600, downtime)             
    for each in BySt:
        print BySt[each]
        downtime = 0
        for ips in BySt[each]['downtime']:
            downtime += BySt[each]['downtime'][ips]
        avg = downtime / len(BySt[each]['downtime'])
        avg = min(3600, avg)
        del BySt[each]['downtime']
        BySt[each]['downtime'] = avg
    return BySt, ByIP, Subs, lastResults

""" Assemble shifts results or other user-defined time periods """
def Shifts(db, nowTime):
    shiftCal = False
    startTime = nowTime
    hr = int(nowTime[8:10]) 
    if hr < 20 and hr >= 8:
        print "Night Shift Starts Being Calculated!"
        startTime = startTime[0:8] + "08" + "0000"
        shiftCal = True
    elif hr > 20:
        print "Day Shift Starts Being Calculated!"
        startTime = startTime[0:8] + "20" + "0000"
        shiftCal = True
    elif hr < 8:
        print "Day Shift Starts Being Calculated!"
        startTime = timeHrAdd(startTime, -24)
        startTime = startTime[0:8] + "20" + "0000"
        shiftCal = True        
    if shiftCal:
        print "------------------------------- SHIFTS -------------------------------"
        shift = "N/A"
        if startTime[8:10] == NightShift:
            shift = "N"
        elif startTime[8:10] == DayShift:
            shift = "D"
        shiftTime = -12            
        newstopsTime = startTime
        newstartTime = timeHrAdd(startTime, shiftTime)        
        subCol = MongoSubStationName_Collection
        ipCol  = MongoStationIP_Collection
        print newstartTime, newstopsTime
        sub, ips, stations = MRmain(db, newstartTime, newstopsTime, subCol, ipCol, shiftTime, shift)
        date = newstartTime[0:8]
        ### Starting the insertion after the deletion for preventing duplicates
        for each in sub: # sub_station_name
            q = {'shift': shift,
                 'date': sub[each]['date'],
                 'station_ip': sub[each]['station_ip'],
                 'sub_station_name': sub[each]['sub_station_name']}
            ret = db.find(BySubStationName_Shifts, q)
            if not ret:
                db.insert(BySubStationName_Shifts, sub[each])
            else:
                sub[each]["downtime"] += ret[0]["downtime"]
                for codes in ret[0]["downtime_error"]:
                    if codes not in sub[each]["downtime_error"]:
                        sub[each]["downtime_error"][codes] = ret[0]["downtime_error"][codes]
                    else:
                        sub[each]["downtime_error"][codes]["count"] += ret[0]["downtime_error"][codes]["count"]
                        sub[each]["downtime_error"][codes]["duration"] += ret[0]["downtime_error"][codes]["duration"]
                db.remove(BySubStationName_Shifts, q)
                db.insert(BySubStationName_Shifts, sub[each])
        for each in ips: # IP
            q = {'shift': shift,
                 'date': ips[each]['date'],
                 'station_ip': ips[each]['station_ip']}
            ret = db.find(ByStationIP_Shifts, q)
            #print "%", ips[each]['station_ip']
            #print ips[each]
            #print ret
            if not ret:
                db.insert(ByStationIP_Shifts, ips[each])
            else:
                ips[each]["downtime"] += ret[0]["downtime"]
                for codes in ret[0]["downtime_error"]:
                    if codes not in ips[each]["downtime_error"]:
                        ips[each]["downtime_error"][codes] = ret[0]["downtime_error"][codes]
                    else:
                        ips[each]["downtime_error"][codes]["count"] += ret[0]["downtime_error"][codes]["count"]
                        ips[each]["downtime_error"][codes]["duration"] += ret[0]["downtime_error"][codes]["duration"]
                db.remove(ByStationIP_Shifts, q)
                db.insert(ByStationIP_Shifts, ips[each])
        for each in stations: # Station Level
            q = {'shift': shift,
                 'date': stations[each]['date'],
                 'station_name': stations[each]['station_name'],
                 'line':stations[each]['line']}
            ret = db.find(ByStation_Shifts, q)
            if not ret:
                db.insert(ByStation_Shifts, stations[each])
                #print stations[each]
            else:
                stations[each]["downtime"] += ret[0]["downtime"]
                db.remove(ByStation_Shifts, q)
                db.insert(ByStation_Shifts, stations[each])
        print "----------------------------------------------------------------------"
            
""" Main of the AE_ETL program """
def main(startTime, resD, db):    
    """ Hbase and main ETL process """
    station_sub, retIP = ETL_Downtime(resD, db)
    BySt, ByIP, Subs, lastResults = AssembleTheResults(station_sub, startTime, retIP)
    
    """ To MongoDB """
    for d in Subs: # Collections of SubStationName
        q = {'time': startTime[8:10]+"0000",
             'date': Subs[d]['date'],
             'station_ip': Subs[d]['station_ip'],
             'sub_station_name': Subs[d]['sub_station_name']}
        ret = db.find(MongoSubStationName_Collection, q)
        if ret:
            dt = ret[0]['downtime']
            dt_err = ret[0]['downtime_error']
            for errcode in Subs[d]['downtime_error']:
                if errcode not in dt_err:
                    dt_err[errcode] = Subs[d]['downtime_error'][errcode]
                else:
                    dt_err[errcode]['count'] += Subs[d]['downtime_error'][errcode]['count']
                    dt_err[errcode]['duration'] += Subs[d]['downtime_error'][errcode]['duration']
            dt += Subs[d]['downtime']
            db.update(MongoSubStationName_Collection, q, {'downtime': dt, 'downtime_error':dt_err})
        else:
            db.insert(MongoSubStationName_Collection, Subs[d])
    for d in ByIP: # Collections of Station_IP
        q = {'time': startTime[8:10]+"0000",
             'date': ByIP[d]['date'],
             'station_ip': ByIP[d]['station_ip']}
        ret = db.find(MongoStationIP_Collection, q)
        if ret:
            dt = ret[0]['downtime']
            dt_err = ret[0]['downtime_error']
            for errcode in ByIP[d]['downtime_error']:
                if errcode not in dt_err:
                    dt_err[errcode] = ByIP[d]['downtime_error'][errcode]
                else:
                    dt_err[errcode]['count'] += ByIP[d]['downtime_error'][errcode]['count']
                    dt_err[errcode]['duration'] += ByIP[d]['downtime_error'][errcode]['duration']
            dt += ByIP[d]['downtime']
            db.update(MongoStationIP_Collection, q, {'downtime': dt, 'downtime_error':dt_err})
        else:
            db.insert(MongoStationIP_Collection, ByIP[d])
    for d in BySt: # Collections of Station
        q = {'time': startTime[8:10]+"0000",
             'date': BySt[d]['date'],
             'station_name': BySt[d]['station_name'],
             'line': BySt[d]['line']}
        ret = db.find(ByStation, q)
        if ret:
            dt = ret[0]['downtime']
            dt += BySt[d]['downtime']
            db.update(ByStation, q, {'downtime': dt})
        else:
            db.insert(ByStation, BySt[d])
    for d in lastResults: # Collections of Last Result
        if lastResults[d]:
            db.remove(LastResult, {"station_ip_sub":d})
            lastResults[d]["station_ip_sub"] = d
            print "**************************** Last Results ****************************"
            print lastResults[d]
            db.insert("LastResult", lastResults[d])

    """ For checking if there are any new emerged data """
    lastCnt = db.find(MongoCheck_Collection, {"datetime": startTime[0:10]+"0000"})
    if lastCnt:
        print "Needs Modification to Check!!"
        query = {"datetime" : startTime[0:10]+"0000"}
        data = {"countD": len(resD)+lastCnt[0]['countD']}
        db.update(MongoCheck_Collection, query, data)
    else:
        db.insert(MongoCheck_Collection, {"datetime": startTime[0:10]+"0000", "countY" : 0, "countD" : len(resD)})
           
    """ Perform checking on the previous n hours """
    if check_switch:
        times = sorted(preCheckTimes(startTime, 24))
        retTimes = preCheck(db, times)
        if len(retTimes) > 0:
            print "### Needs Correction!"
            Correction(hbD, db, retTimes)       
        else:
            print "### No need to correct!"

if __name__ == "__main__":    
    """ If no arguments exist, input startTime from parameter.py """
    if len(sys.argv) < 2:
        print "### Used default parameters for startTime!"
    else:
        startTime = sys.argv[1]
        startTime = startTime[0:10] + "0000"
        startTime = timeHrAdd(startTime, -1)
        stopsTime = timeHrAdd(startTime, 1)
        print "### Used startTime =", startTime
    
    """ Initialize the required Databases and the main function"""
    hbD = ae.hbase(HBaseTable_Downtime, hbaseIP)
    db = ae.mongo(Mongo_N71_DB, mongoIP, mongoPort)

    """ Read the recorded last query time and the latest insert time """    
    LQT = db.find(LastQueryTime, {})
    oldLastQueryTime = LQT[0]["LastQueryTime"]
    print "### IMPORTANT ### LastQueryTime = ", oldLastQueryTime
    resD, newLastQueryTime = hbD.ScanReturnList_InserTime(oldLastQueryTime)
    if resD:
        startEventTime = resD[0]['key'].split("_")[0]
        stopsEventTime = resD[-1]['key'].split("_")[0]
        data = {"LastQueryTime" : timeSecAdd(newLastQueryTime, 1)}
        time = resD[0]['key'].split("_")[0]
        print "### Updated from "+ startEventTime+" to "+stopsEventTime
        while resD:
            try:
                resDTmp = []
                stopsEventTime = timeHrAdd(startEventTime, 1)[0:10] + "0000"
                while time[0:10] == startEventTime[0:10] and time[0:10] != stopsEventTime[0:10]:
                    resDTmp.append(resD[0])
                    del resD[0]
                    if not resD:
                        break
                    time = resD[0]['key'].split("_")[0]
                startTime = startEventTime[0:10] + "0000"
                stopsTime = stopsEventTime[0:10] + "0000"
                print "#############################################################################"
                print "################ Time Periods: "+startTime+"-"+stopsTime+" ################"
                print "#############################################################################"
                main(startTime, resDTmp, db)        
                startEventTime = time 
                db.update(LastQueryTime, {}, data)
            except:
                print startEventTime
                raise
    """ Shifts ans Station Summary Calculations """
    nowtime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    #nowtime = sys.argv[1]
    if nowtime[8:10] == '09' or nowtime[8:10] == '21':
        Shifts(db, nowtime[0:10]+"0000")
    StationSummarize(nowtime[0:10]+"0000", db)
    
    print "### AE ETL Successfully Ended ###"
    
    """ Close the DBs """
    hbD.close()
    db.close()