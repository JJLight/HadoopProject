from subprocess import call
import datetime
import thread
def impalaTest1(userID):
    current1 = datetime.datetime.now()
    strcommand = ["impala-shell","-i","dn1","-q","use tax;refresh tax.tax_access_log_"+userID+"_10g;select host,max(date_time),count(*) from  tax.tax_access_log_"+userID+"_10g group by host limit 20"];
    call(strcommand)
    current2 = datetime.datetime.now()
    current3 = current2 - current1
    print "userID:" +userID + " total second="+str(current3.seconds)
 
 
 def impalaTest2(userID):
    current1 = datetime.datetime.now()
    strcommand = ["impala-shell","-i","dn"+str(int(userID)%4 +1),"-q","use tax;refresh tax.tax_access_log_"+userID+"_10g;select host,max(date_time),count(*) from  tax.tax_access_log_"+userID+"_10g group by host limit 20"];
    call(strcommand)
    current2 = datetime.datetime.now()
    current3 = current2 - current1
    print "userID:" +userID + " total second="+str(current3.seconds)
    
    
    
[thread.start_new_thread(impalaTest,(str(i),)) for i in range(1,21)]