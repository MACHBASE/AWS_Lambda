import os
import re
import time
import json
from machbaseAPI.machbaseAPI import machbase


def lambda_handler(event, context):
    print("machbase connection sample")

    result1 = connect_machbase()
    result2 = insert_data()
    result3 = append_data()


    return {
        'statusCode': 200,
        'body1': json.dumps(result1),
        'body2': json.dumps(result2),
        'body3': json.dumps(result3)
        #'body': json.dumps('Hello from Lambda!')
    }


def append_data():
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
    return db2.result()



def insert_data():
    ip = "127.0.0.1"
    port = 5656
    
    db = machbase()
    
    if db.open(ip,'SYS','MANAGER',port) is 0 :
        return db.result()
    
    db.execute('drop table sample_table')
    
    if db.execute('create table sample_table(d1 short, d2 integer, d3 long, f1 float, f2 double, name varchar(20), text text, bin binary, v4 ipv4, v6 ipv6, dt datetime)') is 0:
        return db.result()

    for i in range(1,10):
        sql = "INSERT INTO SAMPLE_TABLE VALUES ("
        sql += str((i - 5) * 6552) #short
        sql += ","+ str((i - 5) * 42949672) #integer
        sql += ","+ str((i - 5) * 92233720368547758) #long
        sql += ","+ "1.234"+str((i-5)*7) #float
        sql += ","+ "1.234"+str((i-5)*61) #double
        sql += ",'id-"+str(i)+"'" #varchar
        sql += ",'name-"+str(i)+"'" #text
        sql += ",'aabbccddeeff'" #binary
        sql += ",'192.168.0."+str(i)+"'" #ipv4
        sql += ",'::192.168.0."+str(i)+"'" #ipv6
        sql += ",TO_DATE('2015-08-0"+str(i)+"','YYYY-MM-DD')" #date
        sql += ")";

    if db.execute(sql) is 0 :
        return db.result()
    result = db.result()

    print(str(i)+" record inserted.")

    return result

def connect_machbase():
    ip = "127.0.0.1"
    port = 5656
    
    db = machbase()
    
    if db.open(ip,'SYS','MANAGER',port) is 0 :
        return db.result()
        
    if db.execute('select count(*) from m$tables') is 0 :
        return db.result()
        
    result = db.result()
    
    return result


def get_env():
    #path = os.environ['LAMBDA_RUNTIME_DIR']
    #path = os.environ['LD_LIBRARY_PATH']
    #path = os.environ['PATH']
    #pp = os.getcwd()
    #files = os.listdir('/opt/python/lib')
    #result = {}
    #for f in files:
    #    result[f] = f
    print("dummy")
    
