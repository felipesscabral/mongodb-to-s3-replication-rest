import pymongo
from datetime import datetime, timedelta, time
import boto3
from multiprocessing import Pool
import os
import pytz
import gzip
import json
from bson import json_util
import logging
import sys
import os



def setVariables():
    global s3, temp_dir, client, db, packet_history, bucket_name, dt_reference, load_control, windows_time,log_dir, root_dir

    ###########################
    ###########################
    windows_time = 12
    ###########################
    ###########################

    #setting connection string
    client = pymongo.MongoClient('mongodb+srv://<user>:<password>@<server>/?retryWrites=true&readPreference=secondary')
    db = client["<database>"]

    #dsetting database
    packet_history = client['<database>']['<maincollection>']
    load_control = client['<database>']['<control_collection>']

    #setting timezone
    #gmt = pytz.timezone('GMT')

    #check how to configure the boto into server using amazon documentation
    #execution_time = datetime.now().replace(tzinfo=gmt).strftime("%Y-%m-%d")
    s3 = boto3.client('s3')
    
    #we must put the "/" in the end of the path
    root_dir = '/Users/felipe.cabral/Downloads/resultadoParque/'
    #root_dir = '/home/ec2-user/'
    temp_dir = root_dir+'tempResult/'
    log_dir = root_dir+'logs/'
    

    bucket_name='<bucket_name>'
    
def loopProcessGzip(resultLoop):
    global s3
    global temp_dir
    global db
    global bucket_name
    global packet_history
    global load_control

    logging.info(f"Starting {resultLoop[0]['threadName']} - {resultLoop[0]['nRows']} - {datetime.now()}\n")
    updateList = []
    startTime = datetime.now()

    try:

        for i in resultLoop[1:]:

            start_end_time = int(datetime.combine(datetime.strptime(i['dateTime'], '%Y-%m-%d'), time.min).replace(tzinfo=pytz.timezone('GMT')).timestamp())  
            end_date_time =  int(datetime.combine(datetime.strptime(i['dateTime'], '%Y-%m-%d'), time.max).replace(tzinfo=pytz.timezone('GMT')).timestamp())      

            if i["customerId"] == 0:
                query = {
                    "createdAt":{
                        "$gte":i[
                            "startCreatedAt"
                        ],
                        "$lt":i[
                            "endCreatedAt"
                        ]
                    },
                    "dateTime":{
                        "$gte":start_end_time,
                        "$lte": end_date_time
                    },
                    "companyId":{'$exists' : False}
                    ,
                    "customerId":{'$exists' : False}
                    ,
                    "localId":{'$exists' : False}
                }
            else:
                query = {
                    "createdAt":{
                        "$gte":i[
                            "startCreatedAt"
                        ],
                        "$lt":i[
                            "endCreatedAt"
                        ]
                    },
                    "dateTime":{
                        "$gte":start_end_time,
                        "$lte": end_date_time
                    },
                    "companyId":i[
                        "companyId"
                    ],
                    "customerId":i[
                        "customerId"
                    ],
                    "localId":i[
                        "localId"
                    ]
                }
            #print(str(query))
            projection = {'_id':0}
 
            resultado = packet_history.find(query, projection)

            temp_filename=f"{temp_dir}{i['companyId']}_{i['customerId']}_{i['localId']}_{i['_id']}_{i['dateTime']}.json.gz"
            
            temp_s3_file_name=f"exports/{i['companyId']}/{i['customerId']}/{i['localId']}/{i['_id']}/{i['dateTime']}.json.gz"

            with gzip.open(temp_filename, 'wt', encoding="utf-8") as f:
                json.dump(list(resultado), f, default=json_util.default)
            f.close()
            s3.upload_file(temp_filename, bucket_name, temp_s3_file_name)
            os.remove(temp_filename)

            updateList.append(i['_id'])

        set_query = {
                '$set' : 
                    {'flProcessed': True, 
                     'executionStatistic' : {
                         'startTime' : startTime, 
                         'endTime':datetime.now(), 
                         'threadName': resultLoop[0]['threadName'],
                         'nRowsInThread': resultLoop[0]['nRows'] 
                         }
                    }
                }
        update_query = {'_id': {'$in' : updateList}}
        #updating document
        load_control.update_many(update_query, set_query)
        logging.info(f"Process {resultLoop[0]['threadName']} finished - {datetime.now()}")
    except Exception as e:
        #printig log error
        logging.error(f"Error in the Task {resultLoop[0]['threadName']} -> {str(e)}")

        #deleting the last element from my array!
        del updateList[-1]

        #updating my collection
        load_control.update_many(update_query, set_query)
        
        #updating the last element.
        load_control.update({'_id': i['_id'] }, {'flProcessed' : 'error'})
        
def loadControlTable():
    global db, base_time, load_control,packet_history,dt_reference, windows_time
    try:
        logging.info('===== Starting loadControlTable =====')
        current_max_date = -1
        query_last_create_at = [
            {
                '$group': {
                    '_id': None, 
                    'max': {
                        '$max': '$endCreatedAt'
                    }
                }
            }
        ]

        last_created_at = load_control.aggregate(query_last_create_at)

        #executing first time
        for i in last_created_at:
            current_max_date = i['max'] - 10
        
        #valeu must be descomentes when the resturn is null
        #current_max_date = 1682974430
            
        
        if current_max_date == -1 :
            logging.info('First execution')
            start_date = 631152000 #1990-01-01 00:00:00
            end_date = int(datetime.now().timestamp()) - 10 
            filter_query = {
                        '$match': {
                            'createdAt': {
                                '$lt': end_date, 
                            }
                        }
                    }
        else:
            logging.info('Incremental execution')
            start_date = current_max_date
            end_date = int(datetime.now().timestamp()) -10
            filter_query = {
                        '$match': {
                            'createdAt': {
                                '$gte': start_date, 
                                '$lt': end_date
                            }
                        }
                    }


        logging.info(str(filter_query))
        print(str(filter_query))
        pipe_line=[ filter_query , { '$project': { 'filterDate': { '$dateToString': { 'format': '%Y-%m-%d',  'date': { '$toDate': { '$multiply': [ {'$toLong' :'$createdAt'}, 1000 ] } } } }, 'dateTime': { '$dateToString': { 'format': '%Y-%m-%d',  'date': { '$toDate': { '$multiply': [ {'$toLong' :'$dateTime'}, 1000 ] } } } }, 'companyId': { '$ifNull': [ '$companyId', 0 ] },  'customerId': { '$ifNull': [ '$customerId', 0 ] },  'localId': { '$ifNull': [ '$localId', 0 ] } } }, { '$group': { '_id': { 'companyId': '$companyId',  'customerId': '$customerId',  'localId': '$localId', 'dateTime' : '$dateTime', },  'nRows': { '$count': {} } } }, { '$project': { '_id': 0,  'companyId': '$_id.companyId',  'customerId': '$_id.customerId',  'localId': '$_id.localId',  'nRows': 1, 'dateTime' : '$_id.dateTime', } }, { '$addFields': { 'startCreatedAt': start_date,  'endCreatedAt': end_date, 'executionDate' : datetime.now() } }, { '$merge': { 'into': 'load_control' } } ] 
        logging.info(pipe_line)
        print(str(pipe_line))
        packet_history.aggregate(pipe_line,hint='createdAt_1_dateTime_1_companyId_1_customerId_1_localId_1')
        logging.info('===== Finished loadControlTable =====')
    except Exception as e:
        logging.error(f"Error on the *loadControlTable* function ===> {str(e)}")

def biuldPartition():  
    global load_control

    query = {
            '$or': [
                {
                    'flProcessed': False
                }, {
                    'flProcessed': {
                        '$exists': False
                    }
                }
            ]
        }
    projection = {'_id':1, 'executionDate' : 1,'companyId':1, 'customerId':1,'localId':1, 'dateTime':1, 'startCreatedAt':1, 'endCreatedAt':1}

    #resultLoop = load_control.find(query, projection).sort('startCreatedAt', 1).limit(1)
    resultLoop = load_control.find(query, projection).sort('customerId', 1).limit(1000)

    controler = 0

    masterArray = []

    threadArray0 = []
    threadArray1 = []
    threadArray2 = []
    threadArray3 = []
    threadArray4 = []
    threadArray5 = []
    threadArray6 = []
    threadArray7 = []
    threadArray8 = []
    threadArray9 = []
    threadArray10 = []
    threadArray11 = []
    threadArray12 = []
    threadArray13 = []
    
    for i in resultLoop:
        if(controler == 0):
            controler=1
            threadArray0.append(i)
        elif(controler == 1):
            controler=2
            threadArray1.append(i)
        elif(controler == 2):
            controler=3
            threadArray2.append(i)
        elif(controler == 3):
            controler=4
            threadArray3.append(i)
        elif(controler == 4):
            controler=5
            threadArray4.append(i)
        elif(controler == 5):
            controler=6
            threadArray5.append(i)
        elif(controler == 6):
            controler=7
            threadArray6.append(i)
        elif(controler == 7):
            controler=8
            threadArray7.append(i)
        elif(controler == 8):
            controler=9
            threadArray8.append(i)
        elif(controler == 9):
            controler=10
            threadArray9.append(i)
        elif(controler == 10):
            controler=11
            threadArray10.append(i)
        elif(controler == 11):
            controler=12
            threadArray11.append(i)
        elif(controler == 12):
            controler=13
            threadArray12.append(i)
        elif(controler == 13):
            controler=0
            threadArray13.append(i)
  
    threadArray0.insert(0,{'nRows': len(threadArray0), 'threadName': 'threadArray0'})
    threadArray1.insert(0,{'nRows': len(threadArray1), 'threadName': 'threadArray1'})
    threadArray2.insert(0,{'nRows': len(threadArray2), 'threadName': 'threadArray2'})
    threadArray3.insert(0,{'nRows': len(threadArray3), 'threadName': 'threadArray3'})
    threadArray4.insert(0,{'nRows': len(threadArray4), 'threadName': 'threadArray4'})
    threadArray5.insert(0,{'nRows': len(threadArray5), 'threadName': 'threadArray5'})
    threadArray6.insert(0,{'nRows': len(threadArray6), 'threadName': 'threadArray6'})
    threadArray7.insert(0,{'nRows': len(threadArray7), 'threadName': 'threadArray7'})
    threadArray8.insert(0,{'nRows': len(threadArray8), 'threadName': 'threadArray8'})
    threadArray9.insert(0,{'nRows': len(threadArray9), 'threadName': 'threadArray9'})
    threadArray10.insert(0,{'nRows': len(threadArray10), 'threadName': 'threadArray10'})
    threadArray11.insert(0,{'nRows': len(threadArray11), 'threadName': 'threadArray11'})
    threadArray12.insert(0,{'nRows': len(threadArray12), 'threadName': 'threadArray12'})
    threadArray13.insert(0,{'nRows': len(threadArray13), 'threadName': 'threadArray13'})

    """
    logging.info(f"Number of documents {threadArray0[0]['threadName']} => {threadArray0[0]['nRows']}")
    logging.info(f"Number of documents {threadArray1[0]['threadName']} => {threadArray1[0]['nRows']}")
    logging.info(f"Number of documents {threadArray2[0]['threadName']} => {threadArray2[0]['nRows']}")
    logging.info(f"Number of documents {threadArray3[0]['threadName']} => {threadArray3[0]['nRows']}")
    logging.info(f"Number of documents {threadArray4[0]['threadName']} => {threadArray4[0]['nRows']}")
    logging.info(f"Number of documents {threadArray5[0]['threadName']} => {threadArray5[0]['nRows']}")
    logging.info(f"Number of documents {threadArray6[0]['threadName']} => {threadArray6[0]['nRows']}")
    logging.info(f"Number of documents {threadArray7[0]['threadName']} => {threadArray7[0]['nRows']}\n\n")
    """

    masterArray.append(threadArray0)
    masterArray.append(threadArray1)
    masterArray.append(threadArray2)
    masterArray.append(threadArray3)
    masterArray.append(threadArray4)
    masterArray.append(threadArray5)
    masterArray.append(threadArray6)
    masterArray.append(threadArray7)
    masterArray.append(threadArray8)
    masterArray.append(threadArray9)
    masterArray.append(threadArray10)
    masterArray.append(threadArray11)
    masterArray.append(threadArray12)
    masterArray.append(threadArray13)

    callPool(masterArray)

def callPool(masterArray):
    with Pool(initializer=setVariables, processes=14) as pool:
    #define the preferred function to be called in the multiprocessing process
        _ =  pool.map(loopProcessGzip, masterArray)
    
    logging.info(f"Full process fineshed -> {datetime.now()}")

def setLogging():
    global log_dir
    logging.basicConfig(filename=log_dir+'/denox_s3_loader.log', encoding='utf-8', level=logging.INFO)
    logger = logging.getLogger()
    #logger.addHandler(logging.StreamHandler()) # Writes to console
    logger.setLevel(logging.DEBUG)
    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)

def lock():
    global root_dir
    return os.path.exists(root_dir+'executing.lock')

def deleteDocuments():
    global load_control, packet_history

    delete_date = datetime.now() - timedelta(days=6)
    
    #verifing if we had any error in the ingestion process
    valor = load_control.count_documents({'flProcessed' : 'error'})

    logging.info(f"Starting delete function using : ' {delete_date} => {int(delete_date.timestamp())}")

    if valor == 0:
        logging.info('Starting delete process')
        logging.info(str({'createdAt': {'$lt' : int(delete_date.timestamp())}}))
        packet_history.delete_many({'createdAt': {'$lt' : int(delete_date.timestamp())}})
    else:
        logging.warning('The deletion process does not run due to the load_control collection containing a document with a processing error')

if __name__ == '__main__':
    try:
        global base_time, collection_federation, dt_reference,client, root_dir
        setVariables()
        setLogging()
        logging.info('###############################')
        logging.info(f"Starting process - {datetime.now()}")
        
        
        loadControlTable()
        
        
        if len(sys.argv) >1:         
            logging.info(f"Argument detected: {sys.argv[1]}")   
            if sys.argv[1] == 'loadControlTable':
                loadControlTable()
            elif sys.argv[1] == 'biuldPartition':
                #checking if a process is running
                if (lock()==False):
                    os.system(f"echo {datetime.now()} > {root_dir+'executing.lock'}")
                    biuldPartition()
                    
                    #removing locker
                    os.remove(root_dir+'executing.lock')
                else:
                    logging.info('There is already a ** biuldPartition ** process running')
            elif sys.argv[1] == 'deleteDocuments':
                deleteDocuments()
            else:
                logging.info(f"Passed argument value: ==>'{sys.argv[1]}'<== not accepted!.The options are: loadControlTable , biuldPartition or deleteDocuments")
        else:
            logging.info('Argument not detected Please enter at least 1 argument. The options are: loadControlTable , biuldPartition or deleteDocuments')

        logging.info(f"Process finished - {datetime.now()}")
        logging.info('###############################\n\n\n\n\n')
    except Exception as e:
        logging.error(f"Main class exception ===> {str(e)}")
    finally:
        client.close()



