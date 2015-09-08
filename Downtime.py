import sys
import time
import datetime
import happybase
import collections
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
    
def aggregateDowntime(resD, db):
    retIP = collections.defaultdict(dict)
    retSt = collections.defaultdict(dict)
    ## List for standard keys
    Standard_List = ["time mode", "err code", "datetime"]
    station_sub = collections.defaultdict(list)
    IPs = collections.defaultdict(dict)
    startTime = resD[0]['key'].split("_")[0]
    stopsTime = resD[-1]['key'].split("_")[0]
    startHour = int(startTime[8:10])
    stopsHour = int(stopsTime[8:10])
    gapHour = stopsHour - startHour + 1
    print startTime, stopsTime
    for dicts in resD:
        keys = dicts['key'].split("_")
        ip = "_".join(keys[1:-2])
        stationSub = "_".join(keys[1:-1])
        tmp = collections.defaultdict(str)
        info = dicts['cf1:col1'].split(",")
        for items in info:
            ele = items.split(":")
            tmp[ele[0].lower()] = ele[1]
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
        if ip not in IPs:
#            IPs[ip] = [0] * gapHour * 60
             IPs[ip] = [0] * gapHour * 3600
        ## 0->1 0-> both counts
        if not station_sub[stationSub]:
            station_sub[stationSub].append((tmp['datetime'], tmp['time mode']))
        elif tmp['time mode'] == '1' and station_sub[stationSub][-1][1] == '0':
#            lastTime = station_sub[stationSub][-1][0][8:12]
#            thisTime = tmp['datetime'][8:12]
#            lastMins = int(lastTime[0:2]) % startHour * 60 + int(lastTime[2:]) 
#            thisMins = int(thisTime[0:2]) % startHour * 60 + int(thisTime[2:])
            diff1 = timeDiff(startTime[0:10]+"0000", station_sub[stationSub][-1][0])
            diff2 = timeDiff(startTime[0:10]+"0000", tmp['datetime'])
#            diff1 /= 60
#            diff2 /= 60
#            IPs[ip][diff1:diff2+1] = [1]*(diff2 - diff1 + 1)
            IPs[ip][diff1:diff2] = [1]*(diff2 - diff1)
            station_sub[stationSub].append((tmp['datetime'], tmp['time mode']))
        elif tmp['time mode'] == '0' and station_sub[stationSub][-1][1] == '1':
            station_sub[stationSub].append((tmp['datetime'], tmp['time mode']))
        else:
            pass
    for subs in station_sub:
        ip = "_".join(subs.split("_")[0:-1])
#        print subs
#        print station_sub[subs]
        if station_sub[subs][-1][1] == '0':
#            thisTime = station_sub[subs][-1][0][8:12]
#            thisMins = int(thisTime[0:2]) % startHour * 60 + int(thisTime[2:])
#            l = gapHour * 60 - thisMins
#            IPs[ip][thisMins:] = [1] * l
            pass
        if station_sub[subs][0][1] == '1':
#            thisTime = station_sub[subs][0][0][8:12]
#            thisMins = int(thisTime[0:2]) % startHour * 60 + int(thisTime[2:])
            lastResult = db.find(LastResult, {"station_ip_sub": subs})
            if lastResult:
                #try:
                diff1 = timeDiff(startTime[0:10]+"0000", station_sub[subs][0][0])
                #except: 
                #    continue
#                diff1 /= 60
#                IPs[ip][:diff1+1] = [1] * (diff1 + 1)
                IPs[ip][:diff1] = [1] * (diff1)
                lastTime = lastResult[0]['lastTime']
#                print subs, IPs[ip]
                print "### ByStation:", subs, "LastTime=", lastTime
                #diff = int(timeDiff(lastTime, station_sub[subs][0][0])) / 60 + 1
#                diff = int(timeDiff(lastTime, startTime[0:10]+"0000")) / 60 + 1
#                IPs[ip].append(diff)
#        else:
#            IPs[ip].append(0)
    for ips in IPs:
        ipinfo = ips.split("_")
        station = ipinfo[-1]
        lineinfo = ipinfo[1].split("-")
        line = lineinfo[0][2] + "F" + lineinfo[-1][0:2]
        station = line + "_" + station
        downtime = sum(IPs[ips])
#        print IPs[ips]
#        retIP[ips]['downtime'] = downtime * 60
        retIP[ips]['downtime'] = downtime
        if station not in retSt:
            retSt[station] = IPs[ips]
        else:
            for i in range(len(IPs[ips])):
                retSt[station][i] |= IPs[ips][i]
    for stats in retSt:
        downtime = sum(retSt[stats])
        print stats
#        print retSt[stats]
        del retSt[stats]
        retSt[stats]['downtime'] = downtime
#        print retSt[stats]['downtime']
    date = startTime[0:8]
    hour = startHour
#    for each in retSt:
#        st = {}
#        st["date"] = date
#        if len(str(hour)) < 2:
#            st["time"] = "0" + str(hour) + "0000"
#        else:
#            st["time"] = str(hour) + "0000"
#        st["line"] = each.split("_")[0]
#        st["station_name"] = each.split("_")[1]
#        st["downtime"] = retSt[each]["downtime"]
#        db.insert(ByStation, st)
    return retIP, retSt

def StationSummarize(stopsTime, db):
    update_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    downtime_24hour = {}
    downtime_shift = {}
    downtime_day = {}
    downtime_5days = {}
    stopsHour = int(stopsTime[8:10])
    hour24 = timeHrAdd(stopsTime, -24)
    """ downtime_24hour """
    for i in range(24):
        date = hour24[0:8]
        hour = hour24[8:10] + "0000"
        res = db.find(ByStation, {"date":date, "time":hour})
        hour24 = timeHrAdd(hour24, 1)
        if res:
            for doc in res:
                line = doc["line"]
                station = doc["station_name"]
                try:
                    downtime = doc["downtime"]
                except:
                    continue
                key = line + "_" + station
                if key not in downtime_24hour:
                    downtime_24hour[key] = downtime
                else:
                    downtime_24hour[key] += downtime
            for each in downtime_24hour:
                line = each.split("_")[0]
                stat = each.split("_")[1]
                summary = db.find(StationSummary, {"station_name":stat, "line":line})
                if summary:
                    query = {"station_name":stat, "line":line}
                    data = {
                        "update_time": update_time,
                        "downtime_24hour" : downtime_24hour[each]
                    }
                    db.update(StationSummary, query, data)
                else:
                    downtime = downtime_24hour[each]
                    data = {
                        "station_name": stat,
                        "line": line,
                        "update_time": update_time,
                        "downtime_24hour" : downtime_24hour[each],
                        "downtime_5days" : 0,
                        "downtime_shift": 0,
                        "downtime_day": 0
                    }
                    db.insert(StationSummary, data)    
    """ downtime_shift """
    if stopsHour < 20 and stopsHour >= 8:
        lastShift = "N"
        yesterday = timeHrAdd(stopsTime, -24)[0:8]
    else:
        lastShift = "D"
        yesterday = stopsTime[0:8]
    res = db.find(ByStation_Shifts, {"date": yesterday, "shift":lastShift})
    if res:
        for each in res:
            stat = each["station_name"]
            line = each["line"]
            downtime = each["downtime"]
            query = {"station_name":stat, "line":line}
            data = {
                "update_time": update_time,
                "downtime_shift": downtime
            } 
            found = db.find(StationSummary, {"station_name":stat, "line":line})
            if found:          
                db.update(StationSummary, {"station_name":stat, "line":line}, data)
            else:
                db.insert(StationSummary, data)
    """ downtime_day """
    yesterday = timeHrAdd(stopsTime, -24)[0:8]
    yesterday = yesterday + "000000"
    for i in range(24):
        date = yesterday[0:8]
        hour = yesterday[8:10] + "0000"
        res = db.find(ByStation, {"date":date, "time":hour})
        if res:
            for doc in res:
                line = doc["line"]
                station = doc["station_name"]
                try:
                    downtime = doc["downtime"]
                except:
                    continue
                key = line + "_" + station
                if key not in downtime_day:
                    downtime_day[key] = downtime
                else:
                    downtime_day[key] += downtime
            for each in downtime_day:
                line = each.split("_")[0]
                stat = each.split("_")[1]
                summary = db.find(StationSummary, {"station_name":stat, "line":line})
                if summary:
                    query = {"station_name":stat, "line":line}
                    data = {
                        "update_time": update_time,
                        "downtime_day" : downtime_day[each]
                    }
                    db.update(StationSummary, query, data)
                else:
                    downtime = downtime_day[each]
                    data = {
                        "station_name": stat,
                        "line": line,
                        "update_time": update_time,
                        "downtime_day" : downtime_day[each],
                        "downtime_5days" : 0,
                        "downtime_24hour": 0,
                        "downtime_shift": 0,                        
                    }
                    db.insert(StationSummary, data)       
        yesterday = timeHrAdd(yesterday, 1)
    """ downtime_5days """
    yesterday = timeHrAdd(stopsTime, -120)[0:8]
    yesterday = yesterday + "000000"
    for i in range(120):
        date = yesterday[0:8]
        hour = yesterday[8:10] + "0000"
        res = db.find(ByStation, {"date":date, "time":hour})
        if res:
            for doc in res:
                line = doc["line"]
                station = doc["station_name"]
                try:
                    downtime = doc["downtime"]
                except:
                    continue
                key = line + "_" + station
                if key not in downtime_5days:
                    downtime_5days[key] = downtime
                else:
                    downtime_5days[key] += downtime
            for each in downtime_5days:
                line = each.split("_")[0]
                stat = each.split("_")[1]
                summary = db.find(StationSummary, {"station_name":stat, "line":line})
                if summary:
                    query = {"station_name":stat, "line":line}
                    data = {
                        "update_time": update_time,
                        "downtime_5days" : downtime_5days[each]
                    }
                    db.update(StationSummary, query, data)
                else:
                    downtime = downtime_5days[each]
                    data = {
                        "station_name": stat,
                        "line": line,
                        "update_time": update_time,
                        "downtime_5days" : downtime_5days[each],
                        "downtime_24hour": 0,
                        "downtime_shift": 0,
                        "downtime_day": 0
                    }
                    db.insert(StationSummary, data)    
        yesterday = timeHrAdd(yesterday, 1)         
            
if __name__ == "__main__":
    table = "test"
    hbD = ae.hbase("N71_AE_Downtime", hbaseIP)
    #resD, newLastQueryTime = hbD.ScanReturnList_InserTime("20150428143001")
    resD = hbD.ScanReturnList("20150418110443", "20150418140040")
    db = ae.mongo(Mongo_N71_DB, mongoIP, mongoPort)
    aggregateDowntime(resD, db)
    StationSummarize("20150423000000", db)