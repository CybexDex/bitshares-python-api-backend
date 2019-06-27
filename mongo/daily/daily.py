from logging.handlers import TimedRotatingFileHandler
from pymongo import MongoClient
import json
from bson import json_util
from bson.objectid import ObjectId
import config
from collections import OrderedDict
import logging, time
import datetime
from datetime import timedelta
# import bitshares_websocket_client as bc


logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='/tmp/daily.log',
                filemode='w')
logHandler = TimedRotatingFileHandler(filename = '/tmp/daily.log',
                when = 'D', interval = 1, encoding='utf-8'
)
logger = logging.getLogger('daily')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)

N = 90
client = MongoClient(config.MONGODB_DB_URL)
db = client[config.MONGODB_DB_NAME]

col_name = 'daily'
daily_col = db[col_name]
daily_col_8 = db['daily_8h']
account_history_col = db['account_history']

logger.info('start from '+ str(N) + ' ago!')

def get_last_date_daily():
    try:
        res = list(daily_col.find().sort([('block_date',-1)]).limit(1))[0]
        return res['block_date'][:10]
    except:
        logger.info('no daily yet!!')
        return  (datetime.datetime.utcnow() - timedelta(N,0,0) ).strftime('%Y-%m-%d')
def get_last_date_main():
    try:
        res = list(account_history_col.find().sort([('bulk.block_data.block_num',-1)]).limit(1))[0]
        return res['bulk']['block_data']['block_time'][:10]
    except:
        logger.error('get_last_mongo failed')
        exit(1)
    return res

def get_last_date_main_h():
    try:
        res = list(account_history_col.find().sort([('bulk.block_data.block_num',-1)]).limit(1))[0]
        return res['bulk']['block_data']['block_time'][:13]
    except:
        logger.error('get_last_mongo failed')
        exit(1)
    return res


import traceback
def deal_8(start_t, end_t ):
    start_t = start_t + timedelta(0,8,0)
    end_t = end_t + timedelta(0,8,0)
    i = start_t 
    while 1:
        if i >= end_t:
            logger.info('wait for next date ' + str(i))
            time.sleep(3600)
            end_t =  datetime.datetime.strptime(get_last_date_main(), '%Y-%m-%dT%H')
            continue
        try:
            start = i.strftime('%Y-%m-%dT%H:00:00')
            end = (i + timedelta(1,0,0)).strftime('%Y-%m-%dT%H:00:00')
            logger.info('try to count on ( ' + start + '\t' + end + ')\t' )
            create_acct_num = account_history_col.find({'bulk.operation_type':5, 'bulk.block_data.block_time':{'$gte': start ,'$lt': end }}).count()
            order_create_num = account_history_col.find({'bulk.block_data.block_time':{'$gte':start, '$lt': end }, 'bulk.operation_type':1}).count() 
            fill_order_num = account_history_col.find({'bulk.operation_type':4, 'bulk.block_data.block_time':{'$gte': start ,'$lt': end }}).count()
            pays = list(account_history_col.aggregate([{'$match':{'bulk.operation_type':4, 'bulk.block_data.block_time':{'$gte': start ,'$lt': end }}},{ '$group' : {'_id':'$op.pays.asset_id', 'count':{'$sum': '$op.pays.amount' }}}]))
            receives = list(account_history_col.aggregate([{'$match':{'bulk.operation_type':4, 'bulk.block_data.block_time':{'$gte': start ,'$lt': end }}},{ '$group' : {'_id':'$op.receives.asset_id', 'count':{'$sum': '$op.receives.amount' }}}]))
            def joint(pays, receives):
                turnover_ = {}
                for x in pays:
                    k_ = x['_id'].split('.')[-1]
                    turnover_[k_] = {'pays':  x['count'], 'receives':0}
                for x in receives:
                    k_ = x['_id'].split('.')[-1]
                    turnover_[k_] = turnover_.get(k_,{})
                    turnover_[k_]['pays'] = turnover_[k_].get('pays',0)
                    turnover_[k_]['receives'] = x['count'] 
                for x_ in turnover_.keys():
                    t_ = turnover_[x_]
                    t_['totoal'] = t_['pays'] + t_['receives']
                return turnover_
            turnover_ = joint(pays, receives)
            order_create_account = list(account_history_col.aggregate([ { '$match' : { 'bulk.block_data.block_time':{'$gte':start, '$lt': end }, 'bulk.operation_type': 1 } },  { '$group' : { '_id': '$op.seller'  } }  ]))
            # order_create_account = list(map( lambda x:x['_id'] ,order_create_account))
            order_create_account_num = len(order_create_account)
            asset_create_num = account_history_col.find({'bulk.block_data.block_time':{'$gte':start, '$lt': end }, 'bulk.operation_type':10}).count()
            daily_col_8.save({"block_date":str(i)[:10],"create_acct_num":create_acct_num , "fill_order_num":fill_order_num, "turnover":turnover_ ,'order_create_num':order_create_num, 'order_create_account_num':order_create_account_num, 'asset_create_num':asset_create_num })
            i +=  timedelta(1,0,0)
        except:
            logger.error('fail to query on date '+ str(i) )
            traceback.print_exc()
            break
def deal(start_t, end_t ):
    i = start_t
    while 1:
        if i >= end_t:
            logger.info('wait for next date ' + str(i))
            time.sleep(3600)
            end_t =  datetime.datetime.strptime(get_last_date_main(), '%Y-%m-%d')
            continue
        try:
            start = i.strftime('%Y-%m-%dT00:00:00')
            end = (i + timedelta(1,0,0)).strftime('%Y-%m-%dT00:00:00')
            logger.info('try to count on ( ' + start + '\t' + end + ')\t' )
            create_acct_num = account_history_col.find({'bulk.operation_type':5, 'bulk.block_data.block_time':{'$gte': start ,'$lt': end }}).count()
            order_create_num = account_history_col.find({'bulk.block_data.block_time':{'$gte':start, '$lt': end }, 'bulk.operation_type':1}).count() 
            fill_order_num = account_history_col.find({'bulk.operation_type':4, 'bulk.block_data.block_time':{'$gte': start ,'$lt': end }}).count()
            pays = list(account_history_col.aggregate([{'$match':{'bulk.operation_type':4, 'bulk.block_data.block_time':{'$gte': start ,'$lt': end }}},{ '$group' : {'_id':'$op.pays.asset_id', 'count':{'$sum': '$op.pays.amount' }}}]))
            receives = list(account_history_col.aggregate([{'$match':{'bulk.operation_type':4, 'bulk.block_data.block_time':{'$gte': start ,'$lt': end }}},{ '$group' : {'_id':'$op.receives.asset_id', 'count':{'$sum': '$op.receives.amount' }}}]))
            def joint(pays, receives):
                turnover_ = {}
                for x in pays:
                    k_ = x['_id'].split('.')[-1]
                    turnover_[k_] = {'pays':  x['count'], 'receives':0}
                for x in receives:
                    k_ = x['_id'].split('.')[-1]
                    turnover_[k_] = turnover_.get(k_,{})
                    turnover_[k_]['pays'] = turnover_[k_].get('pays',0)
                    turnover_[k_]['receives'] = x['count'] 
                for x_ in turnover_.keys():
                    t_ = turnover_[x_]
                    t_['totoal'] = t_['pays'] + t_['receives']
                return turnover_
            turnover_ = joint(pays, receives)
            order_create_account = list(account_history_col.aggregate([ { '$match' : { 'bulk.block_data.block_time':{'$gte':start, '$lt': end }, 'bulk.operation_type': 1 } },  { '$group' : { '_id': '$op.seller'  } }  ]))
            # order_create_account = list(map( lambda x:x['_id'] ,order_create_account))
            order_create_account_num = len(order_create_account)
            asset_create_num = account_history_col.find({'bulk.block_data.block_time':{'$gte':start, '$lt': end }, 'bulk.operation_type':10}).count()
            daily_col.save({"block_date":str(i)[:10],"create_acct_num":create_acct_num , "fill_order_num":fill_order_num, "turnover":turnover_ ,'order_create_num':order_create_num, 'order_create_account_num':order_create_account_num, 'asset_create_num':asset_create_num })
            i +=  timedelta(1,0,0)
        except:
            logger.error('fail to query on date '+ str(i) )
            traceback.print_exc()
            break
def run():
    end = datetime.datetime.strptime(get_last_date_main(), '%Y-%m-%d')
    logger.info('last end is ' +str(end))
    start = datetime.datetime.strptime(get_last_date_daily(), '%Y-%m-%d') + timedelta(1,0,0)
    logger.info('last start is ' +str(start))
    deal(start, end)

if __name__ == '__main__':
    run()
