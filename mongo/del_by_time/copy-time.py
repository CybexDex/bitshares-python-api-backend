from pymongo import MongoClient
import json
from bson import json_util
from bson.objectid import ObjectId
import config
from collections import OrderedDict
import logging, time
# from bitshares import BitShares
import datetime
from datetime import datetime,timedelta

logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='/tmp/del_log.log',
                filemode='w')

client = MongoClient(config.MONGODB_DB_URL)
db = client[config.MONGODB_DB_NAME]

logger = logging.getLogger('logger')
def get_first_date():
    try:
        res = list(db.account_history.find().sort([('bulk.block_data.block_time',1)]).limit(1))[0]
        return datetime.strptime(res['bulk']['block_data']['block_time'][:10],'%Y-%m-%d')
    except:
        logger.error('get_first_date failed')
        exit(1)
    return res

def get_last_date():
    try:
        res = list(db.account_history.find().sort([('bulk.block_data.block_time',-1)]).limit(1))[0]
        return res['bulk']['block_data']['block_time'][:10]
    except:
        logger.error('get_last_date failed')
        exit(1)
    return res
N = 54
def deal(startdate):
    first = get_first_date()
    logger.info('first is ' + str(first))
    logger.info('start is ' + str(startdate))
    di = first
    while 1:
        if di >= startdate:
            break
        tmpstart = str(di)[:10]
        tmpend = str(di + timedelta(1,0,0) )[:10]
        col_name = 'account_history_' + di.strftime('%Y%m%d')
        di += timedelta(1,0,0)
        try:
            # db.eval("db.account_history.find({'bulk.block_data.block_time':{$gte: '%s',$lt: '%s'} }).forEach( function(d){ db['%s'].insert(d); } )" % (tmpstart,tmpend, col_name) )
            logger.info(tmpstart + '\t' + tmpend+ '\t' + col_name)
            # db[col_name].drop()
            for it in db.account_history.find({'bulk.block_data.block_time':{"$gte": tmpstart,"$lt": tmpend } })  :
                db[col_name].save(it)
            # db.account_history.find({'bulk.block_data.block_time':{'$gte': tmpstart,'$lt': tmpend} }).forEach( bson.Code("function(d){ db['%s'].insert(d); }" % col_name) ) 
        except:
            logger.error('fail to copy data on ' + tmpstart + ' to new collection '+ col_name)
            continue
    try:
        logger.info('try to delete data before ' +  str(startdate)[:10] )
        # db.account_history.delete_many({'bulk.block_data.block_time':{'$lt': str(startdate)[:10]}})
    except:
        logger.error('fail to delete data before '+ str(startdate)[:10])
def run():
    lastdate = datetime.strptime(get_last_date(),'%Y-%m-%d')
    startdate = lastdate - timedelta(N,0,0)
    deal( startdate )

if __name__ == '__main__':
    run()
    pass
