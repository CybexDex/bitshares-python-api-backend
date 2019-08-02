import datetime
import json
import urllib2
import psycopg2
from services.bitshares_websocket_client import BitsharesWebsocketClient, client as bitshares_ws_client_factory
from services.cache import cache
import config
import logging
from pymongo import MongoClient
import json
from bson import json_util
from bson.objectid import ObjectId
import q,traceback
from logging.handlers import TimedRotatingFileHandler

is_print_date = False
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='/tmp/log/mydebug.log',
                filemode='w')
logHandler = TimedRotatingFileHandler(filename = '/tmp/log/mydebug.log', when = 'D', interval = 1, encoding='utf-8')
logger = logging.getLogger('logger')
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)


client = MongoClient(config.MONGODB_DB_URL)
db = client[config.MONGODB_DB_NAME]
logger.info(config.MONGODB_DB_URL)
logger.info(config.MONGODB_DB_NAME)
# qconn = q.q(host = config.Q_HOST, port = config.Q_PORT, user = config.Q_USER)
###################### Q functions ######################

@cache.memoize(timeout= 30 )    
def EON_pair_rank(baseAsset, quoteAsset):
    qconn = q.q(host = config.Q_HOST, port = config.Q_EON_PORT, user = config.Q_USER)
    if baseAsset == 'null'  or  quoteAsset == 'null':
        return {'msg': 'error params!'}
    sql = 'get_rank[`%s;`%s]' % (baseAsset, quoteAsset)
    try:
        logger.info(sql)
        res = qconn.k(str(sql))
    except:
        logger.error(traceback.format_exc())
        logger.error(sql)
        return []
    qconn.close()
    try:
        # return map(lambda x: {'basset':x[0],'qasset':x[1],'account':x[2],'amount':x[3]},list(res))
        return  list(map(lambda x: {'account':x[0],'total':x[1],'pay':x[2],'receive':x[3]},list(res)))
    except:
        logger.error(sql)
        logger.error(res)
        return {'msg': 'error when parse!'}




@cache.memoize(timeout= 30 )    
def statistic_rank_pair_top(duration, side, ttype, baseAsset, quoteAsset):
    qconn = q.q(host = config.Q_HOST, port = config.Q_PORT, user = config.Q_USER)
    if duration not in (1,12,24):
        return []
    duration = str(duration)
    if side not in ('sell', 'buy'):
        return []
    if ttype not in ('net',''):
        return []
    tbname = '_'.join(['top','pair',ttype,side,duration]).replace('__','_')
    if baseAsset != 'null' and quoteAsset != 'null':
        sql = 'trade[%s];select from %s where (basset=`%s ) and (qasset = `%s) ' % (duration, tbname, baseAsset, quoteAsset) 
        sql2 = 'select basset,qasset,amt from (select sum amt by basset,qasset from %s) where (basset=`%s ) and (qasset = `%s)' % (tbname , baseAsset, quoteAsset) 
    else:
        sql = 'trade[%s];select from %s' % (duration, tbname)
        sql2 = 'select basset,qasset,amt from select sum amt by basset,qasset from %s ' % (tbname ) 
    try:
        logger.info(sql)
        logger.info(sql2)
        res = qconn.k(str(sql))
        res2 = qconn.k(str(sql2))
    except:
        logger.error(traceback.format_exc())
        logger.error(sql)
        return {'ranking':[],'total':[]}
    qconn.close()
    try:
        # return map(lambda x: {'basset':x[0],'qasset':x[1],'account':x[2],'amount':x[3]},list(res))
        return  {'ranking':map(lambda x: {'basset':x[0],'qasset':x[1],'account':x[2],'amount':x[3]},list(res)),'total':map(lambda x: {'basset':x[0],'qasset':x[1],'amount':x[2]},list(res2))}
    except:
        logger.error(sql)
        logger.error(res)
        return ['error']



@cache.memoize(timeout= 30 )    
def statistic_rank_top(duration, side, ttype, asset):
    qconn = q.q(host = config.Q_HOST, port = config.Q_PORT, user = config.Q_USER)
    if duration not in (1,12,24):
        return []
    duration = str(duration)
    if side not in ('sell', 'buy'):
        return []
    if ttype not in ('net',''):
        return []
    tbname = '_'.join(['top',ttype,side,duration]).replace('__','_')
    if asset != 'null':
        sql = 'trade[%s];select from %s where asset=`%s' % (duration, tbname,asset) 
    else:
        sql = 'trade[%s];select from %s' % (duration, tbname)
    try:
        logger.info(sql)
        res = qconn.k(str(sql))
    except:
        res = []
        logger.error(traceback.format_exc())
        logger.error(sql)
    qconn.close()
    return map(lambda x: {'asset':x[0],'account':x[1],'amount':x[2]},list(res))



@cache.memoize(timeout= 3 )    
def get_header():
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    response = bitshares_ws_client.request('database', 'get_dynamic_global_properties', [])
    res = _add_global_informations(response, bitshares_ws_client)
    bitshares_ws_client.close()
    return res

def get_balance(address):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    res = bitshares_ws_client.request('database', 'get_balance_objects', [[address]])
    bitshares_ws_client.close()
    return res


def get_account_count():
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    res = bitshares_ws_client.request('database', 'get_account_count', [])
    bitshares_ws_client.close()
    return res

@cache.memoize(timeout= 3600 * 12 )    
def get_asset_create_num():
    head = get_header()
    start = datetime.datetime.strptime(head['time'][:10] ,'%Y-%m-%d' ) 
    j = db.account_history.find({'bulk.block_data.block_time':{'$gte':str(start)[:10] }, 'bulk.operation_type':10}).count()
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    start_sym = ""
    asset_total_num = 1
    while 1:
        asl = bitshares_ws_client.request('database', 'list_assets', [start_sym,100])
        i = len(asl)
        asset_total_num += i - 1
        start_sym = asl[-1]['symbol']
        if i < 100 :
            break
    # asset_total_num = len(bitshares_ws_client.request('database', 'list_assets', ["",100]))
    bitshares_ws_client.close()
    return asset_total_num - j 
@cache.memoize(timeout= 60)    
def get_asset_create_num_24h_bj():
    # j = db.account_history.find({'bulk.block_data.block_time':{'$gte':str(start)[:10] , '$lt': str(end)[:10]}, 'bulk.operation_type':10}).count()
    try:
        res = list(db.daily_8h.find({}).sort([('block_date',-1)]).limit(1))[0]
        if is_print_date is False:
            return res['asset_create_num']
        j = {}
        j['asset_create_num'] = res['asset_create_num']
        j['block_date'] = res['block_date']
    except:
        try:
            j = db.account_history.find({'bulk.block_data.block_time':{'$gte':str(datetime.datetime.utcnow())[:10]}, 'bulk.operation_type':10}).count()
        except:
            return []
    return j

@cache.memoize(timeout= 60)    
def get_asset_create_num_24h():
    # j = db.account_history.find({'bulk.block_data.block_time':{'$gte':str(start)[:10] , '$lt': str(end)[:10]}, 'bulk.operation_type':10}).count()
    try:
        # j = list( db.daily.find().sort([('block_date',-1)]).limit(1) )
        res = list(db.daily.find({}).sort([('block_date',-1)]).limit(1))[0]
        if is_print_date is False:
            return res['asset_create_num']
        j = {}
        j['asset_create_num'] = res['asset_create_num']
        j['block_date'] = res['block_date']
    except:
        try:
            j = db.account_history.find({'bulk.block_data.block_time':{'$gte':str(datetime.datetime.utcnow())[:10]}, 'bulk.operation_type':10}).count()
        except:
            return []
    return j
@cache.memoize(timeout= 600)    
def get_ordercreate_num_24h_bj():
    try:
        res = list(db.daily_8h.find({}).sort([('block_date',-1)]).limit(1))[0]
        if is_print_date is False:
            return res['order_create_num']
        j = {}
        j['order_create_num'] = res['order_create_num']
        j['block_date'] = res['block_date']
    except:
        j = {}
    return j

@cache.memoize(timeout= 600)    
def get_ordercreate_num_24h():
    try:
        res = list(db.daily.find({}).sort([('block_date',-1)]).limit(1))[0]
        if is_print_date is False:
            return res['order_create_num']
        j = {}
        j['order_create_num'] = res['order_create_num']
        j['block_date'] = res['block_date']
    except:
        j = {}
    return j
@cache.memoize(timeout= 3600 * 12 )    
def get_ordercreate_account_num_24h_bj():
    try:
        res = list(db.daily_8h.find({}).sort([('block_date',-1)]).limit(1))[0]
        if is_print_date is False:
            return max(len(res.get('order_create_account_list', [])), res.get('order_create_account_num',0))
        j = {}
        j['order_create_account_num'] = max(len(res.get('order_create_account_list', [])), res.get('order_create_account_num',0))
        j['block_date'] = res['block_date']
    except:
        j = {}
    return j
@cache.memoize(timeout= 3600 * 12 )    
def get_ordercreate_account_num_24h():
    try:
        res = list(db.daily.find({}).sort([('block_date',-1)]).limit(1))[0]
        if is_print_date is False:
            return max(len(res.get('order_create_account_list', [])), res.get('order_create_account_num',0))
        j = {}
        j['order_create_account_num'] = max(len(res.get('order_create_account_list', [])), res.get('order_create_account_num',0))
        j['block_date'] = res['block_date']
    except:
        j = {}
    return j


@cache.memoize(timeout= 60 )    
def get_asset_turnover_24h():
    qconn = q.q(host = config.Q_HOST, port = config.Q_PORT, user = config.Q_USER)   
    sql = 'trade[24];turnover_24h[]'
    try:
        res = qconn.k(sql)
    except:
        res = []
        logger.error('error when ' + sql)
    qconn.close()
    asset_ids= map(lambda x: x[0],list(res))
    asset_amounts = map(lambda x: float(x[1]) , list(res))
    _supllies = _get_assets_supply(asset_ids)
    _res = zip(asset_ids,asset_amounts,_supllies)
    _res = filter(lambda x : x[2] > 0 , _res)
    logger.info(_res)
    return map(lambda x: {'asset_id':x[0], 'to_rate':x[1] / x[2]}, _res )

@cache.memoize(timeout= 60 )    
def get_asset_turnover_24h_single(asset_id):
    qconn = q.q(host = config.Q_HOST, port = config.Q_PORT, user = config.Q_USER)   
    sql = 'trade[24];turnover_24h_s[`%s]' % (asset_id)
    try:
        res = qconn.k(str(sql))
    except:
        res = [(asset_id,0)]
        logger.error('error when ' + sql)
    qconn.close()
    asset_ids= map(lambda x: x[0],list(res))
    asset_amounts = map(lambda x: float(x[1]) , list(res))
    _supllies = _get_assets_supply(asset_ids)
    _res = zip(asset_ids,asset_amounts,_supllies)
    return map(lambda x: {'asset_id':x[0], 'to_rate':x[1] / x[2]}, _res )



@cache.memoize(timeout= 60 )    
def get_fill_cap_24h():
    qconn = q.q(host = config.Q_HOST, port = config.Q_PORT, user = config.Q_USER)   
    sql = 'trade[24];turnover_24h[]'
    try:
        res = qconn.k(sql)
    except:
        res = []
        logger.error('error when ' + sql)
    qconn.close()
    return list(res)
@cache.memoize(timeout= 3600 )    
def get_fill_cap_day_bj(date):
    try:
        res = list(db.daily_8h.find({'block_date':date}).limit(1))[0]
        j = {}
        to_ = {}
        for k,v in  res['turnover'].items():
            k_ = '1.3.' + k
            to_[k_] = v
        j['turnover'] = to_
        j['block_date'] = res['block_date']
    except:
        logger.error('invalid param date_, format must be %Y-%m-%d')
        return "invalid param date_, format must be %Y-%m-%d"
    # j = list(db.account_history.aggregate([{'$match':{'bulk.operation_type':4, 'bulk.block_data.block_time':{'$gte': start ,'$lt': end }}},{ '$group' : {'_id':'$op.pays.asset_id', 'count':{'$sum': '$op.pays.amount' }}}]))
    return j


@cache.memoize(timeout= 3600 )    
def get_fill_cap_day(date):
    try:
        res = list(db.daily.find({'block_date':date}).limit(1))[0]
        j = {}
        to_ = {}
        for k,v in  res['turnover'].items():
            k_ = '1.3.' + k
            to_[k_] = v
        j['turnover'] = to_
        j['block_date'] = res['block_date']
    except:
        logger.error('invalid param date_, format must be %Y-%m-%d')
        return "invalid param date_, format must be %Y-%m-%d"
    # j = list(db.account_history.aggregate([{'$match':{'bulk.operation_type':4, 'bulk.block_data.block_time':{'$gte': start ,'$lt': end }}},{ '$group' : {'_id':'$op.pays.asset_id', 'count':{'$sum': '$op.pays.amount' }}}]))
    return j

@cache.memoize(timeout= 3600 )    
def get_fill_cap_day_timepoint(date):
    try:
        d = datetime.datetime.strptime(date,'%Y-%m-%dT%H:%M:%S')
        start = date
        end = (d + datetime.timedelta(1,0,0)).strftime('%Y-%m-%dT%H:%M:%S')
    except:
        logger.error('invalid param date_, format must be %Y-%m-%dT%H:%M:%S')
        return "invalid param date_, format must be %Y-%m-%dT%H:%M:%S"
    j = list(db.account_history.aggregate([{'$match':{'bulk.operation_type':4, 'bulk.block_data.block_time':{'$gte': start ,'$lt': end }}},{ '$group' : {'_id':'$op.pays.asset_id', 'count':{'$sum': '$op.pays.amount' }}}]))
    # j = list(db.account_history.aggregate([{'$match':{'bulk.operation_type':4, 'bulk.block_data.block_time':{'$gte': start ,'$lt': end }}},{ '$group' : {'_id':'$op.pays.asset_id', 'pays':{'$sum': '$op.pays.amount' }, 'receives':{'$sum': '$op.receives.amount' }}}]))
    return j
    # return list(map(lambda x: {'_id':x['_id'],'count':x['pays']+x['receives']}, j))
    # j = list(db.account_history.aggregate([{'$match':{'bulk.operation_type':4, 'bulk.block_data.block_time':{'$gte': start ,'$lt': end }}},{ '$group' : {'_id':'$op.pays.asset_id', 'count':{'$sum':  '$op.receives.amount' }}}]))
@cache.memoize(timeout= 60 )    
def get_fill_count_day_bj(date):
    try:
        res = list(db.daily_8h.find({'block_date':date}).limit(1))[0]
        if is_print_date is False:
            return res['fill_order_num'] / 2
        j = {}
        j['fill_order_num'] = res['fill_order_num'] / 2
        j['block_date'] = res['block_date']
    except:
        try:
            j = db.account_history.find({'bulk.operation_type':4, 'bulk.block_data.block_time':{'$gte': date } }).count() / 2
        except:
            logger.error('invalid param date_, format must be %Y-%m-%d and in the range')
            return "invalid param date_, format must be %Y-%m-%d and in the range"
    # j = db.account_history.find({'bulk.operation_type':4, 'bulk.block_data.block_time':{'$gte': start ,'$lt': end } }).count() / 2
    return j
@cache.memoize(timeout= 60 )    
def get_fill_count_day(date):
    try:
        res = list(db.daily.find({'block_date':date}).limit(1))[0]
        if is_print_date is False:
            return res['fill_order_num'] / 2
        j = {}
        j['fill_order_num'] = res['fill_order_num'] / 2
        j['block_date'] = res['block_date']
    except:
        try:
            j = db.account_history.find({'bulk.operation_type':4, 'bulk.block_data.block_time':{'$gte': date } }).count() / 2
        except:
            logger.error('invalid param date_, format must be %Y-%m-%d and in the range')
            return "invalid param date_, format must be %Y-%m-%d and in the range"
    # j = db.account_history.find({'bulk.operation_type':4, 'bulk.block_data.block_time':{'$gte': start ,'$lt': end } }).count() / 2
    return j
 

@cache.memoize(timeout= 60 )    
def get_fill_count_24h():
    qconn = q.q(host = config.Q_HOST, port = config.Q_PORT, user = config.Q_USER)   
    sql = 'trade[24];fill_count[]'
    try:
        res = qconn.k(sql)
    except:
        res = []
        logger.error('error when ' + sql)
    qconn.close()
    return res
@cache.memoize(timeout= 3600 )    
def get_account_count_24h_bj():
    try:
        res = list(db.daily_8h.find().sort([('block_date',-1)]).limit(1))[0]
        if is_print_date is False:
            return res['create_acct_num'] / 2
        j = {}
        j['create_acct_num'] = res['create_acct_num'] / 2
        j['block_date'] = res['block_date']
    except:
        logger.error('invalid param date_, format must be %Y-%m-%d and in the range')
        return []
    # j = db.account_history.find({'bulk.block_data.block_time':{'$gte':str(start)[:10],'$lt':str(end)[:10]}, 'bulk.operation_type':5}).count() / 2 
    return j


@cache.memoize(timeout= 3600 )    
def get_account_count_24h():
    try:
        res = list(db.daily.find().sort([('block_date',-1)]).limit(1))[0]
        if is_print_date is False:
            return res['create_acct_num'] / 2
        j = {}
        j['create_acct_num'] = res['create_acct_num'] / 2
        j['block_date'] = res['block_date']
    except:
        logger.error('invalid param date_, format must be %Y-%m-%d and in the range')
        return []
    # j = db.account_history.find({'bulk.block_data.block_time':{'$gte':str(start)[:10],'$lt':str(end)[:10]}, 'bulk.operation_type':5}).count() / 2 
    return j


def get_account(account_id):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    res = bitshares_ws_client.request('database', 'get_accounts', [[account_id]])
    bitshares_ws_client.close()
    return res

@cache.memoize(timeout= 3)    
def get_account_count_days_store_bj(num):
    res = list(db.daily_8h.find().sort([('block_date',-1)]).limit(num))
    j =  list(map(lambda x:{'block_date':x['block_date'], 'create_acct_num':x['create_acct_num'] / 2},res))
    return j
@cache.memoize(timeout= 3)    
def get_account_count_days_store(num):
    res = list(db.daily.find().sort([('block_date',-1)]).limit(num))
    j =  list(map(lambda x:{'block_date':x['block_date'], 'create_acct_num':x['create_acct_num'] / 2},res))
    return j



@cache.memoize(timeout= 3)    
def get_account_count_days(num):
    _date = (datetime.datetime.now() - datetime.timedelta(num,0,0)).strftime('%Y-%m-%d')
    j = list (db.account_history.aggregate([ { '$match' : { 'bulk.operation_type': 5, 'bulk.block_data.block_time':{'$gte': _date} } },  { '$group' : { '_id': {'year' : { '$year' : { '$toDate' :'$bulk.block_data.block_time'} }, 'month' : { '$month' : {'$toDate':'$bulk.block_data.block_time'} }, 'day' : { '$dayOfMonth' : { '$toDate' :'$bulk.block_data.block_time'}} } , 'count': { '$sum' : 0.5 } } } , { '$sort' :{'_id': 1} } ] ))
    return j

def get_account_name(account_id):
    account = get_account(account_id)
    return account[0]['name']

def get_account_id(account_name):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    if not _is_object(account_name):
        account = bitshares_ws_client.request('database', 'lookup_account_names', [[account_name], 0])
        bitshares_ws_client.close()
        return account[0]['id']
    else:
        return account_name

def _add_global_informations(response, ws_client):
    # get market cap
    core_asset = ws_client.get_object('2.3.0')
    current_supply = core_asset["current_supply"]
    confidental_supply = core_asset["confidential_supply"]
    market_cap = int(current_supply) + int(confidental_supply)
    response["cyb_market_cap"] = int(market_cap/100000000)

    if config.TESTNET != 1: # Todo: had to do something else for the testnet
        cybBtcVolume = ws_client.request('database', 'get_24_volume', ["CYB", "JADE"])
        response["quote_volume"] = cybBtcVolume["quote_volume"]
    else:
        response["quote_volume"] = 0

    global_properties = ws_client.get_global_properties()
    response["commitee_count"] = len(global_properties["active_committee_members"])
    response["witness_count"] = len(global_properties["active_witnesses"])

    return response

def _enrich_operation(operation, ws_client):
    dynamic_global_properties = ws_client.request('database', 'get_dynamic_global_properties', [])
    operation["accounts_registered_this_interval"] = dynamic_global_properties["accounts_registered_this_interval"]
    return _add_global_informations(operation, ws_client)

def get_operation(operation_id):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    operation = bitshares_ws_client.get_object(operation_id)
    if not operation:
        operation = {} 
    
    operation = _enrich_operation(operation, bitshares_ws_client)
    bitshares_ws_client.close()
    return [ operation ]


def get_operation_full(operation_id):
    # lets connect the operations to a full node
    bitshares_ws_full_client = BitsharesWebsocketClient(config.FULL_WEBSOCKET_URL)

    operation = bitshares_ws_full_client.get_object(operation_id)
    if not operation:
        operation = {} 

    operation = _enrich_operation(operation, bitshares_ws_full_client)
    bitshares_ws_full_client.close()
    return [ operation ]


def get_trx(trx_id):
    j = list(db.account_history.find({'bulk.block_data.trx_id':trx_id}))
    results = [0 for x in range(len(j))]
    for n in range(0, len(j)):
        results[n] = {"op": [j[n]['bulk']['operation_type'],j[n]["op"]],
                      "block_num": j[n]['bulk']["block_data"]["block_num"],
                      "id": j[n]['bulk']["account_history"]["operation_id"],
                      "timestamp": j[n]['bulk']["block_data"]["block_time"]
                      }

    return list(results)


def get_order_mongo(account, order_id, optype):
    if optype == 'null':
        j = list(db.account_history.find({'$or':[{'op.seller':account,'result.1':order_id}, {'op.order':order_id}, {'op.order_id':order_id} ] }).sort([('bulk.block_data.block_time',1)]) )
    elif optype == 'create':
        j = list(db.account_history.find({ 'op.seller':account,'result.1':order_id }))
    elif optype == 'cancel':
        j = list(db.account_history.find({ "op.order" :order_id }))
    elif optype == 'fill':
        j = list(db.account_history.find({ 'op.order_id':order_id }))
    else:
        return []
    results = [0 for x in range(len(j))]
    for n in range(0, len(j)):
        results[n] = {"op": [j[n]['bulk']['operation_type'],j[n]["op"]],
            "block_num": j[n]['bulk']["block_data"]["block_num"],
           "id": j[n]['bulk']["account_history"]["operation_id"],
           "order_id": order_id,
           "timestamp": j[n]['bulk']["block_data"]["block_time"],
           'obj_id' : str(j[n]['_id'])
           }           
    return list(results)  

@cache.memoize(timeout= 5 )    
def _get_ops_fill_pair_noaccount(start, end, base, quote, limit, page):
    page = int(page)
    limit_ = int(limit)
    limit_ = _ensure_safe_limit(limit_)    
    skip_ = page * limit_
    if start == 'null':
        start = '2018-01-01'
    if end == 'null':
        end = '2050-01-01'
    j = list (db.account_history.find({ 'bulk.block_data.block_time':{'$gte':start, '$lte':end}, '$or':[{'op.fill_price.base.asset_id':base,'op.fill_price.quote.asset_id':quote}, {'op.fill_price.base.asset_id': quote,'op.fill_price.quote.asset_id': base}] }).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
    c = db.account_history.find({ 'bulk.block_data.block_time':{'$gte':start, '$lte':end}, '$or':[{'op.fill_price.base.asset_id':base,'op.fill_price.quote.asset_id':quote}, {'op.fill_price.base.asset_id': quote,'op.fill_price.quote.asset_id': base}] }).count()
    results = [0 for x in range(len(j))]
    for n in range(0, len(j)):
        results[n] = {"op": [j[n]['bulk']['operation_type'],j[n]["op"]],
                      "block_num": j[n]['bulk']["block_data"]["block_num"],
                      "id": j[n]['bulk']["account_history"]["operation_id"],
                      "timestamp": j[n]['bulk']["block_data"]["block_time"],
		              'obj_id' : str(j[n]['_id'])
                      }
    return [results, c]


@cache.memoize(timeout= 5 )    
def _get_ops_fill_pair_noaccount_strict(start, end, base, quote, limit, page):
    page = int(page)
    limit_ = int(limit)
    limit_ = _ensure_safe_limit(limit_)    
    skip_ = page * limit_
    if start == 'null':
        start = '2018-01-01'
    if end == 'null':
        end = '2050-01-01'
 
    j = list (db.account_history.find({ 'bulk.block_data.block_time':{'$gte':start, '$lte':end}, 'op.fill_price.base.asset_id':base,'op.fill_price.quote.asset_id':quote }).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
    c = db.account_history.find({ 'bulk.block_data.block_time':{'$gte':start, '$lte':end}, 'op.fill_price.base.asset_id':base,'op.fill_price.quote.asset_id':quote}).count()
    results = [0 for x in range(len(j))]
    for n in range(0, len(j)):
        results[n] = {"op": [j[n]['bulk']['operation_type'],j[n]["op"]],
                      "block_num": j[n]['bulk']["block_data"]["block_num"],
                      "id": j[n]['bulk']["account_history"]["operation_id"],
                      "timestamp": j[n]['bulk']["block_data"]["block_time"],
		              'obj_id' : str(j[n]['_id'])
                      }
    return [results, c]

@cache.memoize(timeout= 15 )    
def get_fill_bypair(account,start, end, filter_in, filter_out, limit, page):
    page = int(page)
    limit_ = int(limit)
    limit_ = _ensure_safe_limit(limit_)    
    skip_ = page * limit_
    if start == 'null':
        start = '2018-01-01'
    if end == 'null':
        end = '2050-01-01'
    if (filter_in == 'null' and filter_out == 'null') or (filter_in != 'null' and filter_out != 'null'):
        return []
    if filter_in != 'null':
        if account != 'null':
            j = list(db.account_history.find({'result.pair':{'$in':filter_in.split(',')}, 'bulk.account_history.account':account,'bulk.block_data.block_time':{'$gte':start, '$lte':end}}).sort([('bulk.block_data.block_time',-1)] ).limit(limit_).skip(skip_))
            c = db.account_history.find({'result.pair':{'$in':filter_in.split(',')} ,'bulk.account_history.account':account,'bulk.block_data.block_time':{'$gte':start, '$lte':end}}).count()
        else:
            j = list(db.account_history.find({'result.pair':{'$in':filter_in.split(',')} ,'bulk.block_data.block_time':{'$gte':start, '$lte':end}}).sort([('bulk.block_data.block_time',-1)] ).limit(limit_).skip(skip_))
            c = db.account_history.find({'result.pair':{'$in':filter_in.split(',')} ,'bulk.block_data.block_time':{'$gte':start, '$lte':end}}).count()
    else:
        if account == 'null':
            j = list(db.account_history.find({'result.pair':{'$type':'string','$nin':filter_out.split(',')} ,'bulk.block_data.block_time':{'$gte':start, '$lte':end}}).sort([('bulk.block_data.block_time',-1)] ).limit(limit_).skip(skip_))
            c = db.account_history.find({'result.pair':{'$type':'string','$nin':filter_out.split(',')} ,'bulk.block_data.block_time':{'$gte':start, '$lte':end}}).count()
        else:
            j = list(db.account_history.find({'result.pair':{'$type':'string','$nin':filter_out.split(',')} ,'bulk.account_history.account':account,'bulk.block_data.block_time':{'$gte':start, '$lte':end}}).sort([('bulk.block_data.block_time',-1)] ).limit(limit_).skip(skip_))
            c = db.account_history.find({'result.pair':{'$type':'string','$nin':filter_out.split(',')} ,'bulk.account_history.account':account,'bulk.block_data.block_time':{'$gte':start, '$lte':end}}).count()
    results = [0 for x in range(len(j))]
    for n in range(0, len(j)):
        results[n] = {"op": [j[n]['bulk']['operation_type'],j[n]["op"]],
                      "block_num": j[n]['bulk']["block_data"]["block_num"],
                      "id": j[n]['bulk']["account_history"]["operation_id"],
                      "pair": j[n]['result']["pair"],
                      "timestamp": j[n]['bulk']["block_data"]["block_time"],
		              'obj_id' : str(j[n]['_id'])
                      }
    return [results,c]


@cache.memoize(timeout= 5 )    
def get_ops_fill_pair2(account,start, end, base, quote, limit, page):
    page = int(page)
    limit_ = int(limit)
    limit_ = _ensure_safe_limit(limit_)    
    skip_ = page * limit_

    if start == 'null':
        start = '2018-01-01'
    if end == 'null':
        end = '2050-01-01'
 
    if account == "null":
        res = _get_ops_fill_pair_noaccount(start, end, base, quote, limit, page)
        return res
    if start != 'null' and end != 'null':
        if base == 'null':
            if quote == 'null':
                j = list (db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start, '$lte':end},'bulk.operation_type':4 }).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start, '$lte':end},'bulk.operation_type':4 }).count()
            else:
                return []
        elif quote == 'null':
            return []
        else:    
            j = list (db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start, '$lte':end}, '$or':[{'op.fill_price.base.asset_id':base,'op.fill_price.quote.asset_id':quote}, {'op.fill_price.base.asset_id': quote,'op.fill_price.quote.asset_id': base}] }).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
            c = db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start, '$lte':end}, '$or':[{'op.fill_price.base.asset_id':base,'op.fill_price.quote.asset_id':quote}, {'op.fill_price.base.asset_id': quote,'op.fill_price.quote.asset_id': base}] }).count()
    results = [0 for x in range(len(j))]
    for n in range(0, len(j)):
        results[n] = {"op": [j[n]['bulk']['operation_type'],j[n]["op"]],
                      "block_num": j[n]['bulk']["block_data"]["block_num"],
                      "id": j[n]['bulk']["account_history"]["operation_id"],
                      "timestamp": j[n]['bulk']["block_data"]["block_time"],
		              'obj_id' : str(j[n]['_id'])
                      }
    return [results,c]



@cache.memoize(timeout= 5 )    
def get_ops_fill_pair_strict(account,start, end, base, quote, limit, page):
    page = int(page)
    limit_ = int(limit)
    limit_ = _ensure_safe_limit(limit_)    
    skip_ = page * limit_
    if start == 'null':
        start = '2018-01-01'
    if end == 'null':
        end = '2050-01-01'
 
    if account == "null":
        res = _get_ops_fill_pair_noaccount_strict(start, end, base, quote, limit, page)
        return res
    if start != 'null' and end != 'null':
        if base == 'null':
            if quote == 'null':
                j = list (db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start, '$lte':end},'bulk.operation_type':4 }).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start, '$lte':end},'bulk.operation_type':4 }).count()
            else:
                return []
        elif quote == 'null':
            return []
        else:    
            j = list (db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start, '$lte':end}, 'op.fill_price.base.asset_id':base,'op.fill_price.quote.asset_id':quote}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
            c = db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start, '$lte':end}, 'op.fill_price.base.asset_id':base,'op.fill_price.quote.asset_id':quote}).count()
   
        results = [0 for x in range(len(j))]
    for n in range(0, len(j)):
        results[n] = {"op": [j[n]['bulk']['operation_type'],j[n]["op"]],
                      "block_num": j[n]['bulk']["block_data"]["block_num"],
                      "id": j[n]['bulk']["account_history"]["operation_id"],
                      "timestamp": j[n]['bulk']["block_data"]["block_time"],
		              'obj_id' : str(j[n]['_id'])
                      }
    return [results,c]



@cache.memoize(timeout= 5 )    
def get_ops_fill_pair(account,start, end, base, quote, limit, page):
    page = int(page)
    limit_ = int(limit)
    limit_ = _ensure_safe_limit(limit_)    
    skip_ = page * limit_
    if start == 'null':
        start = '2018-01-01'
    if end == 'null':
        end = '2050-01-01'
 
    if account == "null":
        res = _get_ops_fill_pair_noaccount(start, end, base, quote, limit, page)[0]
        return res
    if start != 'null' and end != 'null':
        if base == 'null':
            if quote == 'null':
                j = list (db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start, '$lte':end},'bulk.operation_type':4 }).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
            else:
                return []
        elif quote == 'null':
            return []
        else:    
            j = list (db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start, '$lte':end}, '$or':[{'op.fill_price.base.asset_id':base,'op.fill_price.quote.asset_id':quote}, {'op.fill_price.base.asset_id': quote,'op.fill_price.quote.asset_id': base}] }).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
    results = [0 for x in range(len(j))]
    for n in range(0, len(j)):
        results[n] = {"op": [j[n]['bulk']['operation_type'],j[n]["op"]],
                      "block_num": j[n]['bulk']["block_data"]["block_num"],
                      "id": j[n]['bulk']["account_history"]["operation_id"],
                      "timestamp": j[n]['bulk']["block_data"]["block_time"],
		              'obj_id' : str(j[n]['_id'])
                      }
    return results



@cache.memoize(timeout= 30 )    
def get_ops_conds_mongo(account,start, end, op_type_id, asset, limit, page):
    page = int(page)
    limit_ = int(limit)
    limit_ = _ensure_safe_limit(limit_)    
    skip_ = page * limit_

    if start == 'null':
        start = '2018-01-01'
    if end == 'null':
        end = '2050-01-01'

    if start != 'null' and end != 'null':
        if op_type_id != -1:
            if asset != 'null':
                j = list (db.account_history.find({'bulk.account_history.account':account,'bulk.operation_type':op_type_id, 'bulk.block_data.block_time':{'$gte':start, '$lte':end},\
                 '$or':[{'op.amount.asset_id':asset },{'op.amount_to_sell.asset_id':asset},{'op.min_to_receive.asset_id':asset},{'op.pays.asset_id':asset},{'op.receives.asset_id':asset}] }).sort([('bulk.block_data.block_num',-1)] \
                ).limit(limit_).skip(skip_))
            else:
                j = list (db.account_history.find({'bulk.account_history.account':account,'bulk.operation_type':op_type_id, 'bulk.block_data.block_time':{'$gte':start, '$lte':end}}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
        else:
            if asset != 'null':
                j = list (db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start, '$lte':end}, \
                '$or':[{'op.amount.asset_id':asset },{'op.amount_to_sell.asset_id':asset},{'op.min_to_receive.asset_id':asset},{'op.pays.asset_id':asset},{'op.receives.asset_id':asset}]  }).sort([('bulk.block_data.block_num',-1)] \
                ).limit(limit_).skip(skip_))
            else:
                j = list (db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start, '$lte':end}}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
       
    results = [0 for x in range(len(j))]
    for n in range(0, len(j)):
        results[n] = {"op": [j[n]['bulk']['operation_type'],j[n]["op"]],
                      "block_num": j[n]['bulk']["block_data"]["block_num"],
                      "id": j[n]['bulk']["account_history"]["operation_id"],
                      "timestamp": j[n]['bulk']["block_data"]["block_time"],
		              'obj_id' : str(j[n]['_id'])
                      }
    return list(results)



@cache.memoize(timeout= 300 )    
def get_ops_conds_mongo_count(account,start, end, op_type_id, asset):
    if start == 'null':
        start = '2018-01-01'
    if end == 'null':
        end = '2050-01-01'

    if start != 'null' and end != 'null':
        if op_type_id != -1:
            if asset != 'null':
                c = db.account_history.find({'bulk.account_history.account':account,'bulk.operation_type':op_type_id, 'bulk.block_data.block_time':{'$gte':start, '$lte':end}, '$or':[{'op.amount.asset_id':asset },{'op.amount_to_sell.asset_id':asset},{'op.min_to_receive.asset_id':asset},{'op.pays.asset_id':asset},{'op.receives.asset_id':asset}] }).count()
            else:
                c = db.account_history.find({'bulk.account_history.account':account,'bulk.operation_type':op_type_id, 'bulk.block_data.block_time':{'$gte':start, '$lte':end}}).count()
        else:
            if asset != 'null':
                c = db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start, '$lte':end}, '$or':[{'op.amount.asset_id':asset },{'op.amount_to_sell.asset_id':asset},{'op.min_to_receive.asset_id':asset},{'op.pays.asset_id':asset},{'op.receives.asset_id':asset}]  }).count()
            else:
                c = db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start, '$lte':end}}).count()
    return c      

@cache.memoize(timeout= 30 )    
def get_ops_conds_mongo2(account,start, end, op_type_id, asset, limit, page):
    page = int(page)
    limit_ = int(limit)
    limit_ = _ensure_safe_limit(limit_)    
    skip_ = page * limit_
    if start == 'null':
        start = '2018-01-01'
    if end == 'null':
        end = '2050-01-01'

    if start != 'null' and end != 'null':
        if op_type_id != -1:
            if asset != 'null':
                j = list (db.account_history.find({'bulk.account_history.account':account,'bulk.operation_type':op_type_id, 'bulk.block_data.block_time':{'$gte':start, '$lte':end}, '$or':[{'op.amount.asset_id':asset },{'op.amount_to_sell.asset_id':asset},{'op.min_to_receive.asset_id':asset},{'op.pays.asset_id':asset},{'op.receives.asset_id':asset}] }).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.account_history.account':account,'bulk.operation_type':op_type_id, 'bulk.block_data.block_time':{'$gte':start, '$lte':end}, '$or':[{'op.amount.asset_id':asset },{'op.amount_to_sell.asset_id':asset},{'op.min_to_receive.asset_id':asset},{'op.pays.asset_id':asset},{'op.receives.asset_id':asset}] }).count()
            else:
                j = list (db.account_history.find({'bulk.account_history.account':account,'bulk.operation_type':op_type_id, 'bulk.block_data.block_time':{'$gte':start, '$lte':end}}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.account_history.account':account,'bulk.operation_type':op_type_id, 'bulk.block_data.block_time':{'$gte':start, '$lte':end}}).count()
        else:
            if asset != 'null':
                j = list (db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start, '$lte':end}, '$or':[{'op.amount.asset_id':asset },{'op.amount_to_sell.asset_id':asset},{'op.min_to_receive.asset_id':asset},{'op.pays.asset_id':asset},{'op.receives.asset_id':asset}]  }).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start, '$lte':end}, '$or':[{'op.amount.asset_id':asset },{'op.amount_to_sell.asset_id':asset},{'op.min_to_receive.asset_id':asset},{'op.pays.asset_id':asset},{'op.receives.asset_id':asset}]  }).count()
            else:
                j = list (db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start, '$lte':end}}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start, '$lte':end}}).count()
       
    results = [0 for x in range(len(j))]
    for n in range(0, len(j)):
        results[n] = {"op": [j[n]['bulk']['operation_type'],j[n]["op"]],
                      "block_num": j[n]['bulk']["block_data"]["block_num"],
                      "id": j[n]['bulk']["account_history"]["operation_id"],
                      "timestamp": j[n]['bulk']["block_data"]["block_time"],
		              'obj_id' : str(j[n]['_id'])
                      }
    return [results,c]


@cache.memoize(timeout= 60 )    
def get_ops_by_transfer_accountspair_mongo(asset, acct_from , acct_to, filter_out_account, page, limit):
    page = int(page)
    limit_ = int(limit)
    limit_ = _ensure_safe_limit(limit_)    
    skip_ = page * limit_

    if filter_out_account == 'null':
        filter_out_accounts_list = []
    else:
        filter_out_accounts_list = filter_out_account.split(',')
    # if acct_from == '1.2.4733' or acct_to == '1.2.4733':
    if acct_from in filter_out_accounts_list or acct_to in filter_out_accounts_list:
        return []
    if acct_from != 'null' and acct_to != 'null':
        if acct_from == acct_to :
            return []
        if acct_from == 'or':
            if asset == 'null':
                j = list(db.account_history.find({'bulk.account_history.account':acct_to, 'bulk.operation_type':0}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
            else:
                j = list(db.account_history.find({'bulk.account_history.account':acct_to, 'bulk.operation_type':0, 'op.amount.asset_id':asset}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
        else: 
            if asset == 'null':
                j = list(db.account_history.find({'bulk.account_history.account':acct_from, 'bulk.operation_type':0, 'op.to':acct_to}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
            else:
                j = list(db.account_history.find({'bulk.account_history.account':acct_from, 'bulk.operation_type':0, 'op.to':acct_to, 'op.amount.asset_id':asset}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
    elif acct_from != 'null':
        if asset == 'null':
            if filter_out_account == 'null':
                j = list(db.account_history.find({'bulk.account_history.account':acct_from, 'bulk.operation_type':0, 'op.from':acct_from}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
            else:
                j = list(db.account_history.find({'bulk.account_history.account':acct_from, 'bulk.operation_type':0, 'op.from':acct_from, 'op.to':{'$nin':filter_out_accounts_list}}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
        else:
            if filter_out_account != 'null':
                j = list(db.account_history.find({'bulk.account_history.account':acct_from, 'bulk.operation_type':0, 'op.from':acct_from, 'op.amount.asset_id':asset , 'op.to':{'$nin':filter_out_accounts_list}}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
            else:
                j = list(db.account_history.find({'bulk.account_history.account':acct_from, 'bulk.operation_type':0, 'op.from':acct_from, 'op.amount.asset_id':asset }).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
    elif acct_to != 'null':
        if asset == 'null':
            if filter_out_account != 'null':
                j = list(db.account_history.find({'bulk.account_history.account':acct_to, 'bulk.operation_type':0, 'op.to':acct_to, 'op.from':{'$nin':filter_out_accounts_list}}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
            else:
                j = list(db.account_history.find({'bulk.account_history.account':acct_to, 'bulk.operation_type':0, 'op.to':acct_to}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
        else:
            if filter_out_account != 'null':
                j = list(db.account_history.find({'bulk.account_history.account':acct_to, 'bulk.operation_type':0, 'op.to':acct_to, 'op.amount.asset_id':asset, 'op.from':{'$nin':filter_out_accounts_list}}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
            else:
                j = list(db.account_history.find({'bulk.account_history.account':acct_to, 'bulk.operation_type':0, 'op.to':acct_to, 'op.amount.asset_id':asset}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
    else:
        if asset == 'null':
            if filter_out_account != 'null':
                j = list(db.account_history.find({'bulk.operation_type':0, 'op.from':{'$nin':filter_out_accounts_list} , 'op.to':{'$nin':filter_out_accounts_list} }).limit(limit_).skip(skip_))
            else:
                logger.info(filter_out_account)
                j = list(db.account_history.find({'bulk.operation_type':0}).limit(limit_).skip(skip_))
        else:
            if filter_out_account != 'null':
                j = list(db.account_history.find({'bulk.operation_type':0, 'op.amount.asset_id':asset, 'op.from':{'$nin':filter_out_accounts_list} , 'op.to':{'$nin':filter_out_accounts_list} }).limit(limit_).skip(skip_))
            else:
                j = list(db.account_history.find({'bulk.operation_type':0, 'op.amount.asset_id':asset}).limit(limit_).skip(skip_))
    results = [0 for x in range(len(j))]
    for n in range(0, len(j)):
        results[n] = {"op": [j[n]['bulk']['operation_type'],j[n]["op"]],
                      "block_num": j[n]['bulk']["block_data"]["block_num"],
                      "id": j[n]['bulk']["account_history"]["operation_id"],
                      "timestamp": j[n]['bulk']["block_data"]["block_time"],
		              'obj_id' : str(j[n]['_id']) }
    return results



@cache.memoize(timeout= 60 )    
def get_ops_by_transfer_accountspair_mongo2(asset, acct_from , acct_to, filter_out_account, page, limit):
    page = int(page)
    limit_ = int(limit)
    limit_ = _ensure_safe_limit(limit_)    
    skip_ = page * limit_
    if filter_out_account == 'null':
        filter_out_accounts_list = []
    else:
        filter_out_accounts_list = filter_out_account.split(',')
    # if acct_from == '1.2.4733' or acct_to == '1.2.4733':
    if acct_from in filter_out_accounts_list or acct_to in filter_out_accounts_list:
        return []
    if acct_from != 'null' and acct_to != 'null':
        if acct_from == acct_to :
            return [0,[]]
        if acct_from == 'or':
            if asset == 'null':
                j = list(db.account_history.find({'bulk.account_history.account':acct_to, 'bulk.operation_type':0}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.account_history.account':acct_to, 'bulk.operation_type':0}).count()
            else:
                j = list(db.account_history.find({'bulk.account_history.account':acct_to, 'bulk.operation_type':0, 'op.amount.asset_id':asset}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.account_history.account':acct_to, 'bulk.operation_type':0, 'op.amount.asset_id':asset}).count()
        else: 
            if asset == 'null':
                j = list(db.account_history.find({'bulk.account_history.account':acct_from, 'bulk.operation_type':0, 'op.to':acct_to}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.account_history.account':acct_from, 'bulk.operation_type':0, 'op.to':acct_to}).count()
            else:
                j = list(db.account_history.find({'bulk.account_history.account':acct_from, 'bulk.operation_type':0, 'op.to':acct_to, 'op.amount.asset_id':asset}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.account_history.account':acct_from, 'bulk.operation_type':0, 'op.to':acct_to, 'op.amount.asset_id':asset}).count()
    elif acct_from != 'null':
        if asset == 'null':
            if filter_out_account == 'null':
                j = list(db.account_history.find({'bulk.account_history.account':acct_from, 'bulk.operation_type':0, 'op.from':acct_from}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.account_history.account':acct_from, 'bulk.operation_type':0, 'op.from':acct_from}).count()
            else:
                j = list(db.account_history.find({'bulk.account_history.account':acct_from, 'bulk.operation_type':0, 'op.from':acct_from, 'op.to':{'$nin':filter_out_accounts_list}}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.account_history.account':acct_from, 'bulk.operation_type':0, 'op.from':acct_from, 'op.to':{'$nin':filter_out_accounts_list}}).count()
        else:
            if filter_out_account != 'null':
                j = list(db.account_history.find({'bulk.account_history.account':acct_from, 'bulk.operation_type':0, 'op.from':acct_from, 'op.amount.asset_id':asset , 'op.to':{'$nin':filter_out_accounts_list}}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.account_history.account':acct_from, 'bulk.operation_type':0, 'op.from':acct_from, 'op.amount.asset_id':asset , 'op.to':{'$nin':filter_out_accounts_list}}).count()
            else:
                j = list(db.account_history.find({'bulk.account_history.account':acct_from, 'bulk.operation_type':0, 'op.from':acct_from, 'op.amount.asset_id':asset }).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.account_history.account':acct_from, 'bulk.operation_type':0, 'op.from':acct_from, 'op.amount.asset_id':asset }).count()
    elif acct_to != 'null':
        if asset == 'null':
            if filter_out_account != 'null':
                j = list(db.account_history.find({'bulk.account_history.account':acct_to, 'bulk.operation_type':0, 'op.to':acct_to, 'op.from':{'$nin':filter_out_accounts_list}}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.account_history.account':acct_to, 'bulk.operation_type':0, 'op.to':acct_to, 'op.from':{'$nin':filter_out_accounts_list}}).count()
            else:
                j = list(db.account_history.find({'bulk.account_history.account':acct_to, 'bulk.operation_type':0, 'op.to':acct_to}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.account_history.account':acct_to, 'bulk.operation_type':0, 'op.to':acct_to}).count()
        else:
            if filter_out_account != 'null':
                j = list(db.account_history.find({'bulk.account_history.account':acct_to, 'bulk.operation_type':0, 'op.to':acct_to, 'op.amount.asset_id':asset, 'op.from':{'$nin':filter_out_accounts_list}}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.account_history.account':acct_to, 'bulk.operation_type':0, 'op.to':acct_to, 'op.amount.asset_id':asset, 'op.from':{'$nin':filter_out_accounts_list}}).count()
            else:
                j = list(db.account_history.find({'bulk.account_history.account':acct_to, 'bulk.operation_type':0, 'op.to':acct_to, 'op.amount.asset_id':asset}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.account_history.account':acct_to, 'bulk.operation_type':0, 'op.to':acct_to, 'op.amount.asset_id':asset}).count()
    else:
        if asset == 'null':
            if filter_out_account != 'null':
                j = list(db.account_history.find({'bulk.operation_type':0, 'op.from':{'$nin':filter_out_accounts_list} , 'op.to':{'$nin':filter_out_accounts_list} }).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.operation_type':0, 'op.from':{'$nin':filter_out_accounts_list} , 'op.to':{'$nin':filter_out_accounts_list} }).count()
            else:
                j = list(db.account_history.find({'bulk.operation_type':0}).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.operation_type':0}).count()
        else:
            if filter_out_account != 'null':
                j = list(db.account_history.find({'bulk.operation_type':0, 'op.amount.asset_id':asset, 'op.from':{'$nin':filter_out_accounts_list} , 'op.to':{'$nin':filter_out_accounts_list} }).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.operation_type':0, 'op.amount.asset_id':asset, 'op.from':{'$nin':filter_out_accounts_list} , 'op.to':{'$nin':filter_out_accounts_list} }).count()
            else:
                j = list(db.account_history.find({'bulk.operation_type':0, 'op.amount.asset_id':asset}).limit(limit_).skip(skip_))
                c = db.account_history.find({'bulk.operation_type':0, 'op.amount.asset_id':asset}).count()
    results = [0 for x in range(len(j))]
    for n in range(0, len(j)):
        results[n] = {"op": [j[n]['bulk']['operation_type'],j[n]["op"]],
                      "block_num": j[n]['bulk']["block_data"]["block_num"],
                      "id": j[n]['bulk']["account_history"]["operation_id"],
                      "timestamp": j[n]['bulk']["block_data"]["block_time"],
		              'obj_id' : str(j[n]['_id']) }
    return [results,c]



def get_operation_full_mongo(operation_id):
    res = list(db.account_history.find({'bulk.account_history.operation_id':operation_id}).limit(1))
    if len(res) ==0:
        return []
    operation = { 
        "op": res[0]["op"],
        "block_num": res[0]['bulk']["block_data"]["block_num"], 
        "block_time": res[0]['bulk']["block_data"]["block_time"]
    }

    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    operation = _enrich_operation(operation, bitshares_ws_client)
    return [ operation ]

@cache.memoize(timeout= 3600 )
def get_accounts():
    bitshares_ws_client = bitshares_ws_client_factory.get_instance(2)
    res = []
    count = 0
    while(1):
        core_asset_holders = bitshares_ws_client.request('asset', 'get_asset_holders', ['1.3.0', count, 100])
        res.extend(core_asset_holders)
        if len(core_asset_holders) < 100:
            count += len(core_asset_holders)
            break
        count += 100
    bitshares_ws_client.close()
    return { 'total':count, 'content': res}


def get_full_accounts(account_ids):
    account_ids = account_ids.split(',')
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    account = bitshares_ws_client.request('database', 'get_full_accounts', [account_ids, 0])
    bitshares_ws_client.close()
    return account


def get_full_account(account_id):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    account = bitshares_ws_client.request('database', 'get_full_accounts', [[account_id], 0])
    bitshares_ws_client.close()
    return account


def get_fees():
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    res = bitshares_ws_client.get_global_properties()
    bitshares_ws_client.close()
    return res


def get_account_history(account_id):
    account_id = get_account_id(account_id)
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    account_history = bitshares_ws_client.request('history', 'get_account_history', [account_id, "1.11.1", 20, "1.11.9999999999"])

    if(len(account_history) > 0):
        for transaction in account_history:
            creation_block = bitshares_ws_client.request('database', 'get_block_header', [str(transaction["block_num"]), 0])
            transaction["timestamp"] = creation_block["timestamp"]
            transaction["witness"] = creation_block["witness"]
    bitshares_ws_client.close()
    return account_history


def get_asset(asset_id):
    return [ _get_asset(asset_id) ]


@cache.memoize(timeout= 60 )    
def get_assets_supply(asset_ids):
    asset_ids = asset_ids.split(',')
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    assets = bitshares_ws_client.request('database', 'get_assets', [asset_ids, 0])
    res_assets = []
    for asset in assets:
        dynamic_asset_data = bitshares_ws_client.get_object(asset["dynamic_asset_data_id"])
        dynamic_asset_data['max_supply'] = asset['options']['max_supply']
        dynamic_asset_data['symbol'] = asset['symbol']
        dynamic_asset_data['precision'] = asset['precision']
        bitasset_data_id = asset.get('bitasset_data_id',None)
        if bitasset_data_id!= None:
            bitasset_data = bitshares_ws_client.get_object(bitasset_data_id)
            asset['_bitasset_data'] = bitasset_data
        dynamic_asset_data['asset_full_info'] = asset
        res_assets.append(dynamic_asset_data)
    bitshares_ws_client.close()
    return res_assets
    

def _get_assets_supply(asset_ids):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    assets = bitshares_ws_client.request('database', 'get_assets', [asset_ids, 0])
    jadepool_balances = bitshares_ws_client.request('database', 'get_full_accounts', [['1.2.4733'], 0])[0][1]['balances']
    jadegateway_balance = dict(map(lambda x: (x['asset_type'],x['balance']), jadepool_balances) )
    res_assets = []
    for asset in assets:
        dynamic_asset_data = bitshares_ws_client.get_object(asset["dynamic_asset_data_id"])
        res_assets.append( float(dynamic_asset_data["current_supply"]) - float(jadegateway_balance.get(asset['id'],0) ) )
    bitshares_ws_client.close()
    return res_assets


def _get_assets(asset_id_or_names,is_object):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    if not is_object: # use id
        assets = bitshares_ws_client.request('database', 'lookup_asset_symbols', [asset_id_or_names, 0])
    else:
        assets = bitshares_ws_client.request('database', 'get_assets', [asset_id_or_names, 0])
    res_assets = []
    for asset in assets:
        dynamic_asset_data = bitshares_ws_client.get_object(asset["dynamic_asset_data_id"])
        asset["current_supply"] = dynamic_asset_data["current_supply"]
        asset["confidential_supply"] = dynamic_asset_data["confidential_supply"]
        asset["accumulated_fees"] = dynamic_asset_data["accumulated_fees"]
        asset["fee_pool"] = dynamic_asset_data["fee_pool"]
        res_assets.append(asset)
    bitshares_ws_client.close()
    return res_assets


def _get_asset(asset_id_or_name):
    asset = None
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    if not _is_object(asset_id_or_name):
        asset = bitshares_ws_client.request('database', 'lookup_asset_symbols', [[asset_id_or_name], 0])[0]
    else:
        asset = bitshares_ws_client.request('database', 'get_assets', [[asset_id_or_name], 0])[0]

    dynamic_asset_data = bitshares_ws_client.get_object(asset["dynamic_asset_data_id"])
    asset["current_supply"] = dynamic_asset_data["current_supply"]
    asset["confidential_supply"] = dynamic_asset_data["confidential_supply"]
    asset["accumulated_fees"] = dynamic_asset_data["accumulated_fees"]
    asset["fee_pool"] = dynamic_asset_data["fee_pool"]

    issuer = bitshares_ws_client.get_object(asset["issuer"])
    asset["issuer_name"] = issuer["name"]
    bitshares_ws_client.close()

    return asset


def get_block_header(block_num):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    block_header = bitshares_ws_client.request('database', 'get_block_header', [block_num, 0])
    bitshares_ws_client.close()

    return block_header

@cache.memoize(timeout= 1)    
def get_peak():
    try:
        res = list(db.peak.find({}).sort([('block_num',-1)]).limit(1))[0]
        j = {}
        j['block_num']= res['block_num']
        j['count']= res['count']
        j['peak']= res['peak']
    except:
        j = []
    return j

def get_trx_num_mongo(block_num):
    try:
        res = list(db.peak.find({'block_num': block_num}))[0]
        j = {}
        j['block_num']= res['block_num']
        j['count']= res['count']
        j['peak']= res['peak']
    except:
        j = []
    return j



@cache.memoize(timeout= 3 )
def get_trx_num(block_num):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    ops = bitshares_ws_client.request('database', 'get_block', [block_num, 0])['transactions']#['operations']
    bitshares_ws_client.close()
    return len(ops)


def get_block(block_num):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    block = bitshares_ws_client.request('database', 'get_block', [block_num, 0])
    bitshares_ws_client.close()
    return block


def get_ticker(base, quote):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    res = bitshares_ws_client.request('database', 'get_ticker', [base, quote])
    bitshares_ws_client.close()
    return res
    return block


def get_volume(base, quote):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    return bitshares_ws_client.request('database', 'get_24_volume', [base, quote])


def get_object(object):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    res = [ bitshares_ws_client.get_object(object) ]
    bitshares_ws_client.close()
    return res

def _ensure_asset_id(asset_id):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    if not _is_object(asset_id):
        asset = bitshares_ws_client.request('database', 'lookup_asset_symbols', [[asset_id], 0])[0]
        bitshares_ws_client.close()
        return asset['id']
    else:
        return asset_id

def get_asset_holders_count(asset_id):
    asset_id = _ensure_asset_id(asset_id)
    bitshares_ws_client = bitshares_ws_client_factory.get_instance(2)
    res = bitshares_ws_client.request('asset', 'get_asset_holders_count', [asset_id])
    bitshares_ws_client.close()
    return res


def get_asset_holders(asset_id, start=0, limit=20):
    asset_id = _ensure_asset_id(asset_id)
    bitshares_ws_client = bitshares_ws_client_factory.get_instance(2)
    asset_holders = bitshares_ws_client.request('asset', 'get_asset_holders', [asset_id, start, limit])
    bitshares_ws_client.close()
    return asset_holders


def get_workers():
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    workers_count = bitshares_ws_client.request('database', 'get_worker_count', [])
    workers = bitshares_ws_client.request('database', 'get_objects', [ [ '1.14.{}'.format(i) for i in range(0, workers_count) ] ])

    # get the votes of worker 1.14.0 - refund 400k
    refund400k = bitshares_ws_client.get_object("1.14.0")
    thereshold =  int(refund400k["total_votes_for"])

    result = []
    for worker in workers:
        if worker:
            worker["worker_account_name"] = get_account_name(worker["worker_account"])
            current_votes = int(worker["total_votes_for"])
            perc = (current_votes*100)/thereshold
            worker["perc"] = perc
            result.append([worker])

    bitshares_ws_client.close()
    result = result[::-1] # Reverse list.
    return result


def _is_object(string):
    return len(string.split(".")) == 3
def _ensure_safe_limit(limit):
    if not limit:
        limit = 10
    elif int(limit) > 50:
        limit = 50
    return limit

def get_order_book(base, quote, limit=False):
    limit = _ensure_safe_limit(limit)    
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    order_book = bitshares_ws_client.request('database', 'get_order_book', [base, quote, limit])
    bitshares_ws_client.close()
    return order_book


def get_margin_positions(account_id):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    margin_positions = bitshares_ws_client.request('database', 'get_margin_positions', [account_id])
    bitshares_ws_client.close()
    return margin_positions


def get_witnesses():
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    witnesses_count = bitshares_ws_client.request('database', 'get_witness_count', [])
    witnesses = bitshares_ws_client.request('database', 'get_objects', [ ['1.6.{}'.format(w) for w in range(0, witnesses_count)] ])
    result = []
    bitshares_ws_client.close()
    for witness in witnesses:
        if witness:
            witness["witness_account_name"] = get_account_name(witness["witness_account"])
            result.append([witness])

    result = sorted(result, key=lambda k: int(k[0]['total_votes']))
    result = result[::-1] # Reverse list.
    return result



def get_committee_members():
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    committee_count = bitshares_ws_client.request('database', 'get_committee_count', [])
    committee_members = bitshares_ws_client.request('database', 'get_objects', [ ['1.5.{}'.format(i) for i in range(0, committee_count)] ])
    bitshares_ws_client.close()

    result = []
    for committee_member in committee_members:
        if committee_member:
            committee_member["committee_member_account_name"] = get_account_name(committee_member["committee_member_account"])
            result.append([committee_member])

    result = sorted(result, key=lambda k: int(k[0]['total_votes']))
    result = result[::-1] # this reverses array

    return result


def get_market_chart_dates():
    base = datetime.date.today()
    date_list = [base - datetime.timedelta(days=x) for x in range(0, 100)]
    date_list = [d.strftime("%Y-%m-%d") for d in date_list]
    return list(reversed(date_list))

@cache.memoize(timeout= 300 )
def get_market_data_hour_from(base, quote, start, end):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    base_asset = bitshares_ws_client.request('database', 'lookup_asset_symbols', [[base], 0])[0]
    base_id = base_asset["id"]
    base_precision = 10**base_asset["precision"]

    quote_asset = bitshares_ws_client.request('database', 'lookup_asset_symbols', [[quote], 0])[0]
    quote_id = quote_asset["id"]
    quote_precision = 10**quote_asset["precision"]

    market_history = bitshares_ws_client.request('history', 'get_market_history', [base_id, quote_id, 3600, start, end])

    bitshares_ws_client.close()
    quote_amt = 0
    base_amt = 0
    for m in market_history:
        quote_amt += int(m['close_quote'])
        base_amt += int(m['close_base'])
    res = {'quote_amt':quote_amt, 'base_amt':base_amt ,'base_asset':base,'quote_asset': quote}
    return res
    # return market_history
 


def get_market_data_hour_dur(base, quote, dur):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    base_asset = bitshares_ws_client.request('database', 'lookup_asset_symbols', [[base], 0])[0]
    base_id = base_asset["id"]
    base_precision = 10**base_asset["precision"]

    quote_asset = bitshares_ws_client.request('database', 'lookup_asset_symbols', [[quote], 0])[0]
    quote_id = quote_asset["id"]
    quote_precision = 10**quote_asset["precision"]

    now = datetime.datetime.utcnow()
    ago = now - datetime.timedelta(seconds=dur*3600)
    market_history = bitshares_ws_client.request('history', 'get_market_history', [base_id, quote_id, 3600, ago.strftime("%Y-%m-%dT%H:%M:%S"), now.strftime("%Y-%m-%dT%H:%M:%S")])

    bitshares_ws_client.close()
    return market_history
 

def get_market_data_dur(base, quote, dur):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    base_asset = bitshares_ws_client.request('database', 'lookup_asset_symbols', [[base], 0])[0]
    base_id = base_asset["id"]
    base_precision = 10**base_asset["precision"]

    quote_asset = bitshares_ws_client.request('database', 'lookup_asset_symbols', [[quote], 0])[0]
    quote_id = quote_asset["id"]
    quote_precision = 10**quote_asset["precision"]

    # now = datetime.date.today()
    now = datetime.datetime.utcnow()
    ago = now - datetime.timedelta(days=dur)
    market_history = bitshares_ws_client.request('history', 'get_market_history', [base_id, quote_id, 86400, ago.strftime("%Y-%m-%dT%H:%M:%S"), now.strftime("%Y-%m-%dT%H:%M:%S")])

    bitshares_ws_client.close()
    return market_history
 
def get_market_chart_data_dur(base, quote, dur):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    base_asset = bitshares_ws_client.request('database', 'lookup_asset_symbols', [[base], 0])[0]
    base_id = base_asset["id"]
    base_precision = 10**base_asset["precision"]

    quote_asset = bitshares_ws_client.request('database', 'lookup_asset_symbols', [[quote], 0])[0]
    quote_id = quote_asset["id"]
    quote_precision = 10**quote_asset["precision"]

    now = datetime.date.today()
    ago = now - datetime.timedelta(days=dur)
    market_history = bitshares_ws_client.request('history', 'get_market_history', [base_id, quote_id, 86400, ago.strftime("%Y-%m-%dT%H:%M:%S"), now.strftime("%Y-%m-%dT%H:%M:%S")])

    bitshares_ws_client.close()
    data = []
    for market_operation in market_history:
        open_quote = float(market_operation["open_quote"])
        high_quote = float(market_operation["high_quote"])
        low_quote = float(market_operation["low_quote"])
        close_quote = float(market_operation["close_quote"])

        open_base = float(market_operation["open_base"])
        high_base = float(market_operation["high_base"])
        low_base = float(market_operation["low_base"])
        close_base = float(market_operation["close_base"])

        open = 1/(float(open_base/base_precision)/float(open_quote/quote_precision))
        high = 1/(float(high_base/base_precision)/float(high_quote/quote_precision))
        low = 1/(float(low_base/base_precision)/float(low_quote/quote_precision))
        close = 1/(float(close_base/base_precision)/float(close_quote/quote_precision))

        ohlc = [open, close, low, high]

        data.append(ohlc)

    return data


def get_market_chart_data(base, quote):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    base_asset = bitshares_ws_client.request('database', 'lookup_asset_symbols', [[base], 0])[0]
    base_id = base_asset["id"]
    base_precision = 10**base_asset["precision"]

    quote_asset = bitshares_ws_client.request('database', 'lookup_asset_symbols', [[quote], 0])[0]
    quote_id = quote_asset["id"]
    quote_precision = 10**quote_asset["precision"]

    now = datetime.date.today()
    ago = now - datetime.timedelta(days=100)
    market_history = bitshares_ws_client.request('history', 'get_market_history', [base_id, quote_id, 86400, ago.strftime("%Y-%m-%dT%H:%M:%S"), now.strftime("%Y-%m-%dT%H:%M:%S")])

    bitshares_ws_client.close()
    data = []
    for market_operation in market_history:

        open_quote = float(market_operation["open_quote"])
        high_quote = float(market_operation["high_quote"])
        low_quote = float(market_operation["low_quote"])
        close_quote = float(market_operation["close_quote"])

        open_base = float(market_operation["open_base"])
        high_base = float(market_operation["high_base"])
        low_base = float(market_operation["low_base"])
        close_base = float(market_operation["close_base"])

        open = 1/(float(open_base/base_precision)/float(open_quote/quote_precision))
        high = 1/(float(high_base/base_precision)/float(high_quote/quote_precision))
        low = 1/(float(low_base/base_precision)/float(low_quote/quote_precision))
        close = 1/(float(close_base/base_precision)/float(close_quote/quote_precision))

        ohlc = [open, close, low, high]

        data.append(ohlc)

    append = [0,0,0,0]
    if len(data) < 99:
        complete = 99 - len(data)
        for c in range(0, complete):
            data.insert(0, append)

    return data

@cache.memoize(timeout = 10)
def agg_op_type():
    j = list( db.account_history.aggregate([{'$group' : {'_id' : "$bulk.operation_type", 'num_tutorial' : {'$sum' : 1}}}]))
    
    results = [0 for x in range(len(j))]
    for n in range(0, len(j)):
        results[n] = {"op_type": j[n]['bulk']["id"],
                      "num": j[n]['bulk']["num_tutorial"]
                      }
    return results

def _get_formatted_proxy_votes(proxies, vote_id):
    return list(map(lambda p : '{}:{}'.format(p['id'], 'Y' if vote_id in p["options"]["votes"] else '-'), proxies))

def lookup_accounts(start):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    accounts = bitshares_ws_client.request('database', 'lookup_accounts', [start, 1000])
    bitshares_ws_client.close()
    return accounts


def get_last_block_number():
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    dynamic_global_properties = bitshares_ws_client.request('database', 'get_dynamic_global_properties', [])
    bitshares_ws_client.close()
    return dynamic_global_properties["head_block_number"]


def get_account_history_pager(account_id, page):
    account_id = get_account_id(account_id)

    # connecting into a full node.
    bitshares_ws_full_client = BitsharesWebsocketClient(config.FULL_WEBSOCKET_URL)

    # need to get total ops for account
    account = bitshares_ws_full_client.request('database', 'get_accounts', [[account_id]])[0]
    statistics = bitshares_ws_full_client.get_object(account["statistics"])

    total_ops = statistics["total_ops"]
    start = total_ops - (20 * int(page))
    stop = total_ops - (40 * int(page))

    if stop < 0:
        stop = 0

    if start > 0:
        account_history = bitshares_ws_full_client.request('history', 'get_relative_account_history', [account_id, stop, 20, start])
        for transaction in account_history:
            block_header = bitshares_ws_full_client.request('database', 'get_block_header', [transaction["block_num"], 0])
            transaction["timestamp"] = block_header["timestamp"]
            transaction["witness"] = block_header["witness"]

        return account_history
    else:
        return ""


def get_realtime_ops(obj_id, limit, direction): # direction is  $gte or $lte
    # from_ = int(page) * 20
    # cond = {"$lt":'ObjectId("5b879de955ef3308b134b571"')}
    limit_ = _ensure_safe_limit(limit)    
    cond = {'$'+direction : ObjectId(obj_id) }
    if "lt" in direction:
    	j = list(db.account_history.find({'_id': cond }).sort([('_id',-1)] ).limit(limit_))
    elif "gt" in direction:
    	j = list(db.account_history.find({'_id': cond }).limit(limit_))
	
    # j = json.loads(contents)
    # return j
    
    results = [0 for x in range(len(j))]
    for n in range(0, len(j)):
        results[n] = {"op": [j[n]['bulk']['operation_type'],j[n]["op"]],
                      "block_num": j[n]['bulk']["block_data"]["block_num"],
                      "id": j[n]['bulk']["account_history"]["operation_id"],
                      "timestamp": j[n]['bulk']["block_data"]["block_time"]
                      }

    return list(results)


@cache.memoize(timeout= 300 )
def get_account_history_pager_mongo_count(account_id ):
    account_id = get_account_id(account_id)
    res = db.account_history.find({'bulk.account_history.account':account_id}).count()
    return res

@cache.memoize(timeout= 2 )    
def get_realtime_pager_mongo(page, limit ):
    limit_ = int(limit)
    limit_ = _ensure_safe_limit(limit_)    
    from_ = int(page) * limit
    j = list(db.account_history.find().sort([('bulk.block_data.block_num',-1)] ).limit(limit_))
    results = [0 for x in range(len(j))]
    for n in range(0, len(j)):
        results[n] = {"op": [j[n]['bulk']['operation_type'], j[n]["op"] ],
                      "block_num": j[n]['bulk']["block_data"]["block_num"],
                      "id": j[n]['bulk']["account_history"]["operation_id"],
                      "timestamp": j[n]['bulk']["block_data"]["block_time"],
		      'obj_id' : str(j[n]['_id'])
                      }
    return list(results)

@cache.memoize(timeout= 30 )    
def get_account_history_pager_mongo(account_id, page, limit ):
    # total = get_account_history_pager_mongo_count(account_id)
    limit_ = int(limit)
    limit_ = _ensure_safe_limit(limit_)    
    from_ = int(page) * limit
    # from_ = total - (int(page) * limit_ + limit_)
    if from_ < 0:
        limit_ = limit + from_
        from_ = 0
    if account_id == 'null':
	    return {}
    j = list(db.account_history.find({'bulk.account_history.account':account_id}).sort([('bulk.block_data.block_num',-1)] ).skip(from_).limit(limit_))
    logger.info("mongo req done: " + account_id + " ;" + str(page) + " ;" + str(limit))
    results = [0 for x in range(len(j))]
    llen = len(j) -  1
    for n in range(0, len(j)):
        results[ n] = {"op": [j[n]['bulk']['operation_type'],j[n]["op"]],
                      "block_num": j[n]['bulk']["block_data"]["block_num"],
                      "id": j[n]['bulk']["account_history"]["operation_id"],
                      "timestamp": j[n]['bulk']["block_data"]["block_time"]
                      }

    return list(results)


def get_limit_orders(base, quote):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    limit_orders = bitshares_ws_client.request('database', 'get_limit_orders', [base, quote, 100])
    bitshares_ws_client.close()
    return limit_orders


def get_call_orders(asset_id):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    call_orders = bitshares_ws_client.request('database', 'get_call_orders', [asset_id, 100])
    bitshares_ws_client.close()
    return call_orders


def get_settle_orders(asset, limit):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    settle_orders = bitshares_ws_client.request('database', 'get_settle_orders', [asset, limit])
    bitshares_ws_client.close()
    return settle_orders


def get_fill_order_history(base, quote):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    fill_order_history = bitshares_ws_client.request('history', 'get_fill_order_history', [base, quote, 100])
    bitshares_ws_client.close()
    return fill_order_history



def get_daily_volume_dex_dates():
    base = datetime.date.today()
    date_list = [base - datetime.timedelta(days=x) for x in range(0, 60)]
    date_list = [d.strftime("%Y-%m-%d") for d in date_list]
    return list(reversed(date_list))


def get_all_asset_holders(asset_id):
    asset_id = _ensure_asset_id(asset_id)

    all = []
    bitshares_ws_client = bitshares_ws_client_factory.get_instance(2)
    asset_holders = bitshares_ws_client.request('asset', 'get_asset_holders', [asset_id, 0, 100])

    all.extend(asset_holders)

    len_result = len(asset_holders)
    start = 100
    while  len_result == 100:
        start = start + 100
        asset_holders = bitshares_ws_client.request('asset', 'get_asset_holders', [asset_id, start, 100])
        len_result = len(asset_holders)
        all.extend(asset_holders)
    bitshares_ws_client.close()

    return all



def get_grouped_limit_orders(quote, base, group=10, limit=False):
    limit = _ensure_safe_limit(limit)    

    base = _ensure_asset_id(base)
    quote = _ensure_asset_id(quote)
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    grouped_limit_orders = bitshares_ws_client.request('orders', 'get_grouped_limit_orders', [base, quote, group, None, limit])

    bitshares_ws_client.close()
    return grouped_limit_orders
