""" Newest Version created on: 04.16.2015 """
import os 
import sys
reload(sys)  
sys.setdefaultencoding('utf8')

### Format of the timestamps   = yyyymmddHHMMSS (ie. 20150327042528)
### Pre-Check process is primarily for checking if ever additonal data have been loaded into
### HBase due to some technical issues encountered by the komodo server, causing the latency
### between the arriving times of the data that supposed to be in the same time period  

""" Descriptions of the variables specified below
mongoIP                        = The IP address of our MongoDB
mongoPort                      = The port of our MongoDB
hbaseIP                        = The IP address of our HBase
check_switch                   = 0/1(off/on) for pre-checking if any additional data come in
HBaseTable_Downtime            = Downtime table in HBase
HBaseTable_Yield               = Yield table in HBase
Mongo_N71_DB                   = The N71 AE db in MongoDB, all related collections contained
MongoCheck_Collection          = The collection for information required by the pre-check
MongoSubStationName_Collection = The collection for the sub_station_name section, hourly 
MongoStationIP_Collection      = The collection for the station_ip section, hourly
BySubStationName_Shifts        = The collection for the sub_station_name section w.r.t shifts
ByStationIP_Shifts             = The collection for the station_ip section w.r.t shifts
BySubStationName_Rank          = The collection for the top 5 errors in sub_station_name section
ByStationIP_Rank               = The collection for the top 5 errors in station_ip section
LastResult                     = Only those whose lastModes were 0 were sent into this collection
LastQueryTime                  = The last time slots the AE_ETL runs at
startTime                      = The starting timestamps for the whole process
stopsTime                      = The stopping timestamps for the whole process
DayShift                       = Ending hours of the Day Shift   => from this-12 to this
NightShift                     = Ending hours of the Night shift => from this-12 to this
"""
macaron01                      = '172.28.146.20' # AE-webservice
PTD_SH_CS1                     = '172.28.138.62' # sql-server 
mongoIP                        = macaron01 
mongoPort                      = 27017
hbaseIP                        = '172.28.146.11'
check_switch                   = 0
HBaseTable_Downtime            = "N71_AE_Downtime"
HBaseTable_Yield               = "N71_AE_Yield"
Mongo_N71_DB                   = "N71_AE_Hourly"
MongoCheck_Collection          = "Check"
MongoSubStationName_Collection = "BySubStationName" 
MongoStationIP_Collection      = "ByStationIP"
BySubStationName_Shifts        = "BySubStationName_Shifts"
ByStationIP_Shifts             = "ByStationIP_Shifts"   
BySubStationName_Rank          = "BySubStationName_Rank"
ByStationIP_Rank               = "ByStationIP_Rank"
StationSummary                 = "StationSummary"
ByStation                      = "ByStation"
ByStation_Shifts               = "ByStation_Shifts"
LastResult                     = "LastResult"
LastQueryTime                  = "LastQueryTime"
startTime                      = "20150423100011"
stopsTime                      = "20150423110000"
DayShift                       = "20"
NightShift                     = "08"