import os
import re
import time
import json
from machbaseAPI.machbaseAPI import machbase


def lambda_handler(event, context):
    #("machbase append sample")

    ip = "127.0.0.1"
    port = 5656
    tablename = 'TAG'
    records = 10

    
    # get the column information of TAG table
    db = machbase()
    if db.open(ip,'SYS','MANAGER',port) is 0 :
        return db.result()
        
    
    db.columns(tablename)
    result = db.result()
    
    if db.close() is 0:
        return db.result()

    types = []
    for item in re.findall('{[^}]+}',result):
        types.append(json.loads(item).get('type'))
        
    sStart = time.time()
    
    ## append start
    db2 = machbase()
    if db2.open(ip,'SYS','MANAGER',port) is 0 :
        return db2.result()
    
    if db2.appendOpen(tablename, types) is 0:
        return db2.result()
    
    values = []
    for i in range(0, records):
        v = []
        v.append("TAG-"+str(i))
        v.append("2021-05-18 15:26:"+str(i%40+10))
        v.append(float((i+2)/(i+i+i+1))*1000)
        
        values.append(v)
        
        if (i % 10000) == 0:
            print(i),
        if (i % 1000) == 0:
            continue
        
        if db2.appendData(tablename, types, values) is 0:
            return db2.result()
        values = []
 
    if len(values) > 0:
        if db2.appendData(tablename, types, values) is 0:
            return db2.result()
            
    if db2.appendClose() is 0:
        return db2.result()
    # append end
    result = db2.result()

    return {
        'statusCode': 200,
        'body2': json.dumps(result),
        #'body': json.dumps('Hello from Lambda!')
    }



