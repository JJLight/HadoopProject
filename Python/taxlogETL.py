import sys
import urlparse
import os
for line in sys.stdin:
    #f.write(line)
    line = line.strip()
    client_ip,client,userid,request,status,bytes_sent, date_time,referer,useragent,host = line.split('\t')
    if(len(request.split(' ')) == 3):
        method,uri,protocal = request.split(' ')
    else:
        method = "-"
        uri = "-"
        protocal = "-"
    pr = urlparse.urlparse(uri)
    path = pr.path
    params = pr.params
    query = pr.query
    fileType = os.path.splitext(path)[-1]
    if fileType:
        fileName = path.split("/")[-1]
    else:
        fileName = "-"
        fileType = "-"

    print "\t".join([client_ip,client,userid,request,method,uri,protocal,path,params,query,fileType,fileName,status,bytes_sent, date_time,referer,useragent,host])