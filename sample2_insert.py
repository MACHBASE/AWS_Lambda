import os
import time
import json
from machbaseAPI.machbaseAPI import machbase


def lambda_handler(event, context):
    #("machbase insert_data sample")

    ip = "123.143.222.106"
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


    return {
        'statusCode': 200,
        'body': json.dumps(result),
        #'body': json.dumps('Hello from Lambda!')
    }
