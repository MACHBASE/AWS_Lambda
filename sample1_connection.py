import os
import time
import json
from machbaseAPI.machbaseAPI import machbase

def lambda_handler(event, context):
    print("machbase connection sample")

    db = machbase()
    if db.open('123.143.222.106','SYS','MANAGER',5656) is 0 :
        result = db.result()

    if db.execute('select * from m$tables') is 0 :
        return db.result()
    
    result = db.result()


    return {
        'statusCode': 200,
        'body': json.dumps(result)
        #'body': json.dumps('Hello from Lambda!')
    }


