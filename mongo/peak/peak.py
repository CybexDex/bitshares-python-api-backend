from logging.handlers import TimedRotatingFileHandler
from pymongo import MongoClient
import json
from bson import json_util
from bson.objectid import ObjectId
import config
from collections import OrderedDict
import logging, time
import datetime
from datetime import datetime,timedelta
import bitshares_websocket_client as bc


logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='/tmp/peak.log',
                filemode='w')
logHandler = TimedRotatingFileHandler(filename = '/tmp/peak.log',
                when = 'D', interval = 1, encoding='utf-8'
)
logger = logging.getLogger('peak')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)


client = MongoClient(config.MONGODB_DB_URL)
db = client[config.MONGODB_DB_NAME]

col_name = 'peak'
def get_last_peak():
    try:
        res = list(db[col_name].find().sort([('block_num',-1)]).limit(1))[0]
        return int(res['peak'])
    except:
        logger.info('no peak yet!!')
        return  0
def get_last_blk_mongo():
    try:
        res = list(db[col_name].find().sort([('block_num',-1)]).limit(1))[0]
        return int(res['block_num'])
    except:
        logger.error('get_last_mongo failed')
        return 0
    return res

def get_last_blk():
    ws_clent = bc.client.get_instance()
    try:
        res = ws_clent.request('database', 'get_dynamic_global_properties', [])['last_irreversible_block_num']
    except:
        logger.error('get_last_chain failed')
        exit(1)
    ws_clent.close()
    return int(res)

def deal(start, end, peak):
    i = start
    ws_clent = bc.client.get_instance()
    while 1:
        if i >= end :
            logger.info('wait for next block')
            time.sleep(3)
            end =  get_last_blk()
            continue
        try:
            logger.info('try to count on block ' + str(i))
            res = ws_clent.request('database', 'get_block', [i])
            if res is None:
                c = 0
            else:
                c = len(ws_clent.request('database', 'get_block', [i])['transactions'])
            peak = max(c, peak)
            db[col_name].save({"block_num":i,"count":c, "peak":peak})
            i += 1
        except:
            logger.error('fail to query on block '+ str(i) )
            break
    ws_clent.close()
def run():
    end = get_last_blk()
    peak = get_last_peak()
    start = get_last_blk_mongo()
    logger.info('last start is ' +str(start))
    logger.info('last end is ' +str(end))
    logger.info('last peak is ' +str(peak))
    deal(start, end, peak)

if __name__ == '__main__':
    run()
