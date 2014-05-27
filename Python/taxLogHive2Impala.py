import datetime
import hadoopy
from subprocess import call
#hive -e 'use tax;add jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib-0.10.0-cdh4.4.0.jar;drop table tax_access_log;CREATE TABLE tax_access_log ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe" STORED AS RCFile AS SELECT client_ip,client,userid,request,status,bytes_sent, from_unixtime(unix_timestamp(date_time,"dd/MMM/yyyy:hh:mm:ss")) as date_time,referer,useragent,host FROM tax_access_log_raw where date_time is not NULL;'
#impala-shell -i 'dn1' -q 'use tax;refresh tax_access_log;select host,max(date_time),count(*) from  tax.tax_access_log group by host limit 20';
t1 = datetime.datetime.now()-datetime.timedelta(hours=3)
t2 = datetime.datetime.now()-datetime.timedelta(hours=2)
t3 = datetime.datetime.now()-datetime.timedelta(hours=1)
# example '2014-05-26 18%'
queryDate = [t1.strftime('%Y-%m-%d')+'%'] + [t2.strftime('%Y-%m-%d')+'%'] + [t3.strftime('%Y-%m-%d')+'%']
deleteTime = [t1.strftime('%Y-%m-%d %H')+'%3A00%3A00'] + [t2.strftime('%Y-%m-%d %H')+'%3A00%3A00'] + [t3.strftime('%Y-%m-%d %H')+'%3A00%3A00']
insertTime = [t1.strftime('%Y-%m-%d %H')+'%'] + [t2.strftime('%Y-%m-%d %H')+'%'] + [t3.strftime('%Y-%m-%d %H')+'%']


#hiveStatementForCreate = "use tax;add jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib-0.10.0-cdh4.4.0.jar;drop table tax_access_log_textFile;CREATE TABLE tax_access_log_textFile AS SELECT client_ip,client,userid,request,status,bytes_sent, from_unixtime(unix_timestamp(date_time,'dd/MMM/yyyy:hh:mm:ss')) as date_time,referer,useragent,host FROM tax_access_log_raw where date_time is not NULL;"
#####1.trans datetime format.
hiveStatementForCreate = "use tax;"
hiveStatementForCreate += "add jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib-0.10.0-cdh4.4.0.jar;"
hiveStatementForCreate += "drop table tax_access_log_textFile;"
hiveStatementForCreate += "CREATE TABLE tax_access_log_textFile AS SELECT client_ip,client,userid,request,status,bytes_sent, from_unixtime(unix_timestamp(date_time,'dd/MMM/yyyy:HH:mm:ss')) as date_time,referer,useragent,host FROM tax_access_log_raw"
hiveStatementForCreate += " where date_time is not NULL "
hiveStatementForCreate += " AND("
tempStatement =[]
for query_date in queryDate :
    tempStatement += [" from_unixtime(unix_timestamp(date_time,'dd/MMM/yyyy:HH:mm:ss')) like '"+query_date+"'"]

hiveStatementForCreate += " or ".join(tempStatement)
hiveStatementForCreate += ");"

print "hiveStatementForCreate:"+hiveStatementForCreate;
hivestrcommand = ["hive","-e",hiveStatementForCreate]
current1 = datetime.datetime.now() 
call(hivestrcommand)
current2 = datetime.datetime.now()
print "hive1 second="+str((current2 - current1).seconds)


#hiveStatementForPythonCreate = "use tax;add FILE /home/hadoop/taxlogETL/taxlogETL.py;drop table tax_access_log_python;CREATE TABLE tax_access_log_python  AS select TRANSFORM (client_ip,client,userid,request,status,bytes_sent, date_time,referer,useragent,host )  USING 'python taxlogETL.py'  AS (client_ip,client,userid,request,method,uri,protocal,path,params,query,fileType,fileName,status,bytes_sent,date_time,referer,useragent,host ) FROM tax_access_log_textFile where date_time is not NULL;"
#####2.use python ( max last 2 day)
hiveStatementForPythonCreate = "use tax;"
hiveStatementForPythonCreate += "add FILE /home/hadoop/taxlogETL/taxlogETL.py;"
hiveStatementForPythonCreate += "drop table tax_access_log_python;"
hiveStatementForPythonCreate += "CREATE TABLE tax_access_log_python "
hiveStatementForPythonCreate += " AS select TRANSFORM (client_ip,client,userid,request,status,bytes_sent, date_time,referer,useragent,host ) "
hiveStatementForPythonCreate += " USING 'python taxlogETL.py' "
hiveStatementForPythonCreate += " AS (client_ip,client,userid,request,method,uri,protocal,path,params,query,fileType,fileName,status,bytes_sent,date_time,referer,useragent,host ) "
hiveStatementForPythonCreate += " FROM tax_access_log_textFile where date_time is not NULL "
hiveStatementForPythonCreate += " AND ("
tempStatement =[]
for query_date_time in queryDate :
    tempStatement += ["date_time like '"+query_date_time+"'"]

hiveStatementForPythonCreate += " or ".join(tempStatement)
hiveStatementForPythonCreate += ");"

print "hiveStatementForPythonCreate:"+hiveStatementForPythonCreate;
hivestrcommandForPython = ["hive","-e",hiveStatementForPythonCreate]
current2 = datetime.datetime.now()
call(hivestrcommandForPython)
current3 = datetime.datetime.now()
print "hive2 second="+str((current3 - current2).seconds)

#impalaStatementForCreate = "use tax;refresh tax.tax_access_log_python;insert overwrite TABLE tax_access_log_partition PARTITION (date_hour) SELECT client_ip,client,userid,request,method,uri,protocal,path,params,query,fileType,fileName,status,bytes_sent, date_time,referer,useragent,host,concat(strleft(from_unixtime(unix_timestamp(date_time)),14),'00:00')as date_hour from  tax.tax_access_log_python;";
#####3.delete old data
for deltime in deleteTime :
    hdfsFilePath = '"/user/hive/warehouse/tax.db/tax_access_log_partition/date_hour='+deltime+'"'
    if hadoopy.exists(hdfsFilePath) == 1:
        print "remove file path:"+hdfsFilePath
        hadoopy.rmr('"/user/hive/warehouse/tax.db/tax_access_log_partition/date_hour='+deltime+'"')

#####4.insert Impala
impalaStatementForCreate = "use tax;refresh tax.tax_access_log_python;"
impalaStatementForCreate += " insert into TABLE tax_access_log_partition PARTITION (date_hour) "
impalaStatementForCreate += " SELECT client_ip,client,userid,request,method,uri,protocal,path,params,query,fileType,fileName,status,bytes_sent, date_time,referer,useragent,host,concat(strleft(from_unixtime(unix_timestamp(date_time)),14),'00:00')as date_hour "
impalaStatementForCreate += " from  tax.tax_access_log_python"
impalaStatementForCreate += " where "

tempStatement =[]
for insert_time in insertTime :
    tempStatement += ["date_time like '"+insert_time+"'"]

impalaStatementForCreate += " or ".join(tempStatement)
impalaStatementForCreate += ";"

print "impalaStatementForCreate:"+impalaStatementForCreate;
impalaStrcommand = ["impala-shell","-i","dn1","-q",impalaStatementForCreate];
current2 = datetime.datetime.now()
call(impalaStrcommand)
current3 = datetime.datetime.now()
print "impala1 second="+str((current3 - current2).seconds)

#####5.check Impala
impalaStatementForSelect = "use tax;refresh tax.tax_access_log_partition;select host,max(date_time),count(*) from  tax.tax_access_log_partition group by host limit 20;";
impalaStrcommand = ["impala-shell","-i","dn1","-q",impalaStatementForSelect];
current2 = datetime.datetime.now()
call(impalaStrcommand)
current3 = datetime.datetime.now()
print "impala2 second ="+str((current3 - current2).seconds)

print "total second = "+str((current3 - current1).seconds)