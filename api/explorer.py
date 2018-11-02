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

logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='mydebug.log',
                filemode='w')

client = MongoClient(config.MONGODB_DB_URL)
db = client[config.MONGODB_DB_NAME]

logging.info(config.MONGODB_DB_URL)
logging.info(config.MONGODB_DB_NAME)
# qconn = q.q(host = config.Q_HOST, port = Q_PORT, user = Q_USER)

def get_header():
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    response = bitshares_ws_client.request('database', 'get_dynamic_global_properties', [])
    return _add_global_informations(response, bitshares_ws_client)

def get_account(account_id):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    return bitshares_ws_client.request('database', 'get_accounts', [[account_id]])

def get_account_name(account_id):
    account = get_account(account_id)
    return account[0]['name']

def get_account_id(account_name):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    if not _is_object(account_name):
        account = bitshares_ws_client.request('database', 'lookup_account_names', [[account_name], 0])
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
    return [ operation ]


def get_operation_full(operation_id):
    # lets connect the operations to a full node
    bitshares_ws_full_client = BitsharesWebsocketClient(config.FULL_WEBSOCKET_URL)

    operation = bitshares_ws_full_client.get_object(operation_id)
    if not operation:
        operation = {} 

    operation = _enrich_operation(operation, bitshares_ws_full_client)
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



def get_ops_fill_pair(account,start, end, base, quote, limit, page):
    page = int(page)
    limit_ = int(limit)
    skip_ = page * limit_
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
    elif start == 'null' and end == 'null':
        if base == 'null':
            if quote == 'null':
                j = list (db.account_history.find({'bulk.account_history.account':account, 'bulk.operation_type':4 }).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
            else:
                return []
        elif quote == 'null':
            return []
        else:    
            j = list (db.account_history.find({'bulk.account_history.account':account, '$or':[{'op.fill_price.base.asset_id':base,'op.fill_price.quote.asset_id':quote},{'op.fill_price.base.asset_id':quote ,'op.fill_price.quote.asset_id':base}] }).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
    
    elif start != 'null' and end == 'null': 
        if base == 'null':
            if quote == 'null':
                j = list (db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start},'bulk.operation_type':4 }).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
            else:
                return []
        elif quote == 'null':
            return []
        else:    
            j = list (db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start}, '$or':[{'op.fill_price.base.asset_id':base,'op.fill_price.quote.asset_id':quote },{'op.fill_price.base.asset_id':quote ,'op.fill_price.quote.asset_id':base}]}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
 
    elif start == 'null' and end != 'null':
        if base == 'null':
            if quote == 'null':
                j = list (db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$lte':end},'bulk.operation_type':4 }).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
            else:
                return []
        elif quote == 'null':
            return []
        else:    
            j = list (db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$lte':end}, '$or':[{'op.fill_price.base.asset_id':base,'op.fill_price.quote.asset_id':quote},{'op.fill_price.base.asset_id':quote ,'op.fill_price.quote.asset_id':base}] }).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
    results = [0 for x in range(len(j))]
    for n in range(0, len(j)):
        results[n] = {"op": [j[n]['bulk']['operation_type'],j[n]["op"]],
                      "block_num": j[n]['bulk']["block_data"]["block_num"],
                      "id": j[n]['bulk']["account_history"]["operation_id"],
                      "timestamp": j[n]['bulk']["block_data"]["block_time"],
		              'obj_id' : str(j[n]['_id'])
                      }
    return list(results)



def get_ops_conds_mongo(account,start, end, op_type_id, asset, limit, page):
    page = int(page)
    limit_ = int(limit)
    skip_ = page * limit_
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
    elif start == 'null' and end != 'null':
        if op_type_id != -1:
            if asset != 'null':
                j = list (db.account_history.find({'bulk.account_history.account':account,'bulk.operation_type':op_type_id, 'bulk.block_data.block_time':{ '$lte':end},\
                 '$or':[{'op.amount.asset_id':asset },{'op.amount_to_sell.asset_id':asset},{'op.min_to_receive.asset_id':asset},{'op.pays.asset_id':asset},{'op.receives.asset_id':asset}] }).sort([('bulk.block_data.block_num',-1)] \
                ).limit(limit_).skip(skip_))
            else:
                j = list (db.account_history.find({'bulk.account_history.account':account,'bulk.operation_type':op_type_id, 'bulk.block_data.block_time':{ '$lte':end}}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
        else:
            if asset != 'null':
                j = list (db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{ '$lte':end}, \
                '$or':[{'op.amount.asset_id':asset },{'op.amount_to_sell.asset_id':asset},{'op.min_to_receive.asset_id':asset},{'op.pays.asset_id':asset},{'op.receives.asset_id':asset}]  }).sort([('bulk.block_data.block_num',-1)] \
                ).limit(limit_).skip(skip_))
            else:
                j = list (db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$lte':end}}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
    elif start != 'null' and end == 'null':
        if op_type_id != -1:
            if asset != 'null':
                j = list (db.account_history.find({'bulk.account_history.account':account,'bulk.operation_type':op_type_id, 'bulk.block_data.block_time':{ '$gte':start},\
                 '$or':[{'op.amount.asset_id':asset },{'op.amount_to_sell.asset_id':asset},{'op.min_to_receive.asset_id':asset},{'op.pays.asset_id':asset},{'op.receives.asset_id':asset}] }).sort([('bulk.block_data.block_num',-1)] \
                ).limit(limit_).skip(skip_))
            else:
                j = list (db.account_history.find({'bulk.account_history.account':account,'bulk.operation_type':op_type_id, 'bulk.block_data.block_time':{ '$gte':start}}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
        else:
            if asset != 'null':
                j = list (db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{ '$gte':start}, \
                '$or':[{'op.amount.asset_id':asset },{'op.amount_to_sell.asset_id':asset},{'op.min_to_receive.asset_id':asset},{'op.pays.asset_id':asset},{'op.receives.asset_id':asset}]  }).sort([('bulk.block_data.block_num',-1)] \
                ).limit(limit_).skip(skip_))
            else:
                j = list (db.account_history.find({'bulk.account_history.account':account, 'bulk.block_data.block_time':{'$gte':start}}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
    elif start == 'null' and end == 'null':
        if op_type_id != -1:
            if asset != 'null':
                j = list (db.account_history.find({'bulk.account_history.account':account,'bulk.operation_type':op_type_id, '$or':[{'op.amount.asset_id':asset },{'op.amount_to_sell.asset_id':asset},{'op.min_to_receive.asset_id':asset},{'op.pays.asset_id':asset},{'op.receives.asset_id':asset}] }).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
            else:
                j = list (db.account_history.find({'bulk.account_history.account':account,'bulk.operation_type':op_type_id }).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
        else:
            if asset != 'null':
                j = list (db.account_history.find({'bulk.account_history.account':account, '$or':[{'op.amount.asset_id':asset },{'op.amount_to_sell.asset_id':asset},{'op.min_to_receive.asset_id':asset},{'op.pays.asset_id':asset},{'op.receives.asset_id':asset}] }).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
            else:
                j = list (db.account_history.find({'bulk.account_history.account':account }).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
        
    results = [0 for x in range(len(j))]
    for n in range(0, len(j)):
        results[n] = {"op": [j[n]['bulk']['operation_type'],j[n]["op"]],
                      "block_num": j[n]['bulk']["block_data"]["block_num"],
                      "id": j[n]['bulk']["account_history"]["operation_id"],
                      "timestamp": j[n]['bulk']["block_data"]["block_time"],
		              'obj_id' : str(j[n]['_id'])
                      }
    return list(results)


def get_ops_by_transfer_accountspair_mongo(acct_from , acct_to, page, limit):
    page = int(page)
    limit_ = int(limit)
    skip_ = page * limit_
    if acct_from != 'null' and acct_to != 'null':
        j = list(db.account_history.find({'bulk.account_history.account':acct_from, 'bulk.operation_type':0, 'op.to':acct_to}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
    elif acct_from != 'null':
        j = list(db.account_history.find({'bulk.account_history.account':acct_from, 'bulk.operation_type':0, 'op.from':acct_from}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
    elif acct_to != 'null':
        j = list(db.account_history.find({'bulk.account_history.account':acct_to, 'bulk.operation_type':0, 'op.to':acct_to}).sort([('bulk.block_data.block_num',-1)] ).limit(limit_).skip(skip_))
    else:
        j = []
    results = [0 for x in range(len(j))]
    for n in range(0, len(j)):
        results[n] = {"op": [j[n]['bulk']['operation_type'],j[n]["op"]],
                      "block_num": j[n]['bulk']["block_data"]["block_num"],
                      "id": j[n]['bulk']["account_history"]["operation_id"],
                      "timestamp": j[n]['bulk']["block_data"]["block_time"],
		              'obj_id' : str(j[n]['_id']) }
    return list(results)

def get_operation_full_mongo(operation_id):
    res = list(db.account_history.find({'bulk.account_history.operation_id':operation_id}).limit(1))
    if len(res) ==0:
        return []
    operation = { 
        # "op": json.loads(res[0]["operation_history"]["op"]),
        "op": res[0]["op"],
        "block_num": res[0]['bulk']["block_data"]["block_num"], 
#        "op_in_trx": res[0]["operation_history"]["op_in_trx"],
#        "result": json.loads(res[0]["operation_history"]["operation_result"]), 
#        "trx_in_block": res[0]["operation_history"]["trx_in_block"],
#        "virtual_op": res[0]["operation_history"]["virtual_op"], 
        "block_time": res[0]['bulk']["block_data"]["block_time"]
    }

    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    operation = _enrich_operation(operation, bitshares_ws_client)
    return [ operation ]

def get_accounts():
    bitshares_ws_client = bitshares_ws_client_factory.get_instance(2)
    core_asset_holders = bitshares_ws_client.request('asset', 'get_asset_holders', ['1.3.0', 0, 100])
    return core_asset_holders


def get_full_account(account_id):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    account = bitshares_ws_client.request('database', 'get_full_accounts', [[account_id], 0])
    return account


def get_fees():
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    return bitshares_ws_client.get_global_properties()


def get_account_history(account_id):
    account_id = get_account_id(account_id)
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    account_history = bitshares_ws_client.request('history', 'get_account_history', [account_id, "1.11.1", 20, "1.11.9999999999"])

    if(len(account_history) > 0):
        for transaction in account_history:
            creation_block = bitshares_ws_client.request('database', 'get_block_header', [str(transaction["block_num"]), 0])
            transaction["timestamp"] = creation_block["timestamp"]
            transaction["witness"] = creation_block["witness"]
    try:
        return account_history
    except:
        return {}


def get_asset(asset_id):
    return [ _get_asset(asset_id) ]


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

    return asset


def get_block_header(block_num):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    block_header = bitshares_ws_client.request('database', 'get_block_header', [block_num, 0])
    return block_header


def get_block(block_num):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    block = bitshares_ws_client.request('database', 'get_block', [block_num, 0])
    return block


def get_ticker(base, quote):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    return bitshares_ws_client.request('database', 'get_ticker', [base, quote])


def get_volume(base, quote):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    return bitshares_ws_client.request('database', 'get_24_volume', [base, quote])


def get_object(object):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    return [ bitshares_ws_client.get_object(object) ]

def _ensure_asset_id(asset_id):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    if not _is_object(asset_id):
        asset = bitshares_ws_client.request('database', 'lookup_asset_symbols', [[asset_id], 0])[0]
        return asset['id']
    else:
        return asset_id

def get_asset_holders_count(asset_id):
    asset_id = _ensure_asset_id(asset_id)
    bitshares_ws_client = bitshares_ws_client_factory.get_instance(2)
    return bitshares_ws_client.request('asset', 'get_asset_holders_count', [asset_id])


def get_asset_holders(asset_id, start=0, limit=20):
    asset_id = _ensure_asset_id(asset_id)
    bitshares_ws_client = bitshares_ws_client_factory.get_instance(2)
    asset_holders = bitshares_ws_client.request('asset', 'get_asset_holders', [asset_id, start, limit])
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
    return order_book


def get_margin_positions(account_id):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    margin_positions = bitshares_ws_client.request('database', 'get_margin_positions', [account_id])
    return margin_positions


def get_witnesses():
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    witnesses_count = bitshares_ws_client.request('database', 'get_witness_count', [])
    witnesses = bitshares_ws_client.request('database', 'get_objects', [ ['1.6.{}'.format(w) for w in range(0, witnesses_count)] ])
    result = []
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

@cache.memoize()
def agg_op_type():
    j = list( db.account_history.aggregate([{'$group' : {'_id' : "$bulk.operation_type", 'num_tutorial' : {'$sum' : 1}}}]))
    # j = json.loads(contents)
    # return j
    
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
    return accounts


def get_last_block_number():
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    dynamic_global_properties = bitshares_ws_client.request('database', 'get_dynamic_global_properties', [])
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
    cond = {'$'+direction : ObjectId(obj_id) }
    if "lt" in direction:
    	j = list(db.account_history.find({'_id': cond }).sort([('_id',-1)] ).limit(limit))
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


def get_account_history_pager_mongo_count(account_id ):
    account_id = get_account_id(account_id)
    res = db.account_history.find({'bulk.account_history.account':account_id}).count()
    return res

@cache.memoize(timeout= 1 )    
def get_realtime_pager_mongo(page, limit ):
    limit_ = int(limit)
    from_ = int(page) * limit
    # j = list(db.account_history.find().sort([('_id',-1)] ).limit(limit_))
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

def get_account_history_pager_mongo(account_id, page, limit ):
    # total = get_account_history_pager_mongo_count(account_id)
    limit_ = int(limit)
    from_ = int(page) * limit
    # from_ = total - (int(page) * limit_ + limit_)
    if from_ < 0:
        limit_ = limit + from_
        from_ = 0
    if account_id == 'null':
	    return {}
    # logging.info("request: " + account_id + " ;" + str(from_) + " ;" + str(limit_) + " ;" + str(total))
    # account_id = get_account_id(account_id)
    j = list(db.account_history.find({'bulk.account_history.account':account_id}).sort([('bulk.block_data.block_num',-1)] ).skip(from_).limit(limit_))
    # j = list(db.account_history.find({'bulk.account_history.account':account_id}).skip(from_).limit(limit_))
    logging.info("mongo req done: " + account_id + " ;" + str(page) + " ;" + str(limit))
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
    return limit_orders


def get_call_orders(asset_id):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    call_orders = bitshares_ws_client.request('database', 'get_call_orders', [asset_id, 100])
    return call_orders


def get_settle_orders(base, quote):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    settle_orders = bitshares_ws_client.request('database', 'get_settle_orders', [base, quote, 100])
    return settle_orders


def get_fill_order_history(base, quote):
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    fill_order_history = bitshares_ws_client.request('history', 'get_fill_order_history', [base, quote, 100])
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

    return all



def get_grouped_limit_orders(quote, base, group=10, limit=False):
    limit = _ensure_safe_limit(limit)    

    base = _ensure_asset_id(base)
    quote = _ensure_asset_id(quote)
    bitshares_ws_client = bitshares_ws_client_factory.get_instance()
    grouped_limit_orders = bitshares_ws_client.request('orders', 'get_grouped_limit_orders', [base, quote, group, None, limit])

    return grouped_limit_orders
