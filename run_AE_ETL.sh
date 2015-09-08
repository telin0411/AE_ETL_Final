#!/bin/bash
# Create a new folder for each date when the hour indicates 01:00
STARTTIME=`date +%Y%m%d%H%M%S`
LOGTIME=`date +%Y%m%d%H%M`
DATE=`date +%Y%m%d`
HOUR=`date +%H`
if [ $HOUR -eq "01" ]; then
	mkdir /home/albert/AE_ETL/logs/$DATE
fi
/usr/bin/python /home/albert/AE_ETL/N71_AE_v3.py > /home/albert/AE_ETL/logs/$DATE/$LOGTIME.log
