import sys
import urlparse
import os
import httpagentparser
agentParser = httpagentparser
# You need install httpagentparser in each datanode.
#git clone https://github.com/shon/httpagentparser.git
#cd httpagentparser/
#su
#yum install  python-setuptools
#sudo python setup.py install


#add parseUserAgent
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
        
    simple_data = agentParser.simple_detect(useragent)
    rich_data = agentParser.detect(useragent)
    
    agent_os = simple_data[0]
    agent_browser = simple_data[1]
    browserName = str(rich_data['browser']['name']) if (('browser' in rich_data) and ('name' in rich_data['browser'])) else "-"
    browserVersion = str(rich_data['browser']['version']) if (('browser' in rich_data) and ('version' in rich_data['browser'])) else "-"
    
     #like iOS , Windows ,Android
    platformName = str(rich_data['platform']['name']) if (('platform' in rich_data) and ('name' in rich_data['platform'])) else "-"
    platformVersion = str(rich_data['platform']['version'])   if (('platform' in rich_data) and ('version' in rich_data['platform'])) else "-"
    #like iPhone, Ipad
    distName = str(rich_data['dict']['name']) if (('dict' in rich_data) and ('name' in rich_data['dict'])) else "-"
    distVersion = str(rich_data['dict']['version']) if (('dict' in rich_data) and ('version' in rich_data['dict'])) else "-"

    bot = str(rich_data['bot']) if ('bot' in rich_data) else "-"
    
    print "\t".join([client_ip,client,userid,request,method,uri,protocal,path,params,query,fileType,fileName,status,bytes_sent, date_time,referer,useragent,agent_os,agent_browser,browserName,browserVersion,platformName,platformVersion,distName,distVersion,bot,host])
    
