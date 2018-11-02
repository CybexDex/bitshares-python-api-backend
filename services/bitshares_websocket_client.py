from websocket import create_connection
import json
from services.cache import cache
import logging

class RPCError(Exception):
    pass

class BitsharesWebsocketClient():
    def __init__(self, websocket_url):
        self.ws = create_connection(websocket_url)
        self.request_id = 1
        self.api_ids = {
            'database': 0,
            'login': 1
        }
    
    def request(self, api, method_name, params):
        api_id = self.load_api_id(api)
        payload = {
            'id': self.request_id,
            'method': 'call',
            'params': [
                api_id,
                method_name,
                params
            ]
        }
        request_string = json.dumps(payload) 
        logging.info('> {}'.format(request_string))
        #print('> {}'.format(request_string))
	try:
            self.ws.send(request_string)
            self.request_id += 1
	except:
	    raise RPCError(request_string)
	try:
            reply =  self.ws.recv()
	except:
	    raise RPCError("ws.recv error when send req: " + request_string)
        #print('< {}'.format(reply))

        ret = {}
        try:
            ret = json.loads(reply, strict=False)
        except ValueError:
            raise ValueError("Client returned invalid format. Expected JSON!")

        
        if 'error' in ret:
	    print request_string
            if 'detail' in ret['error']:
                raise RPCError(ret['error']['detail'])
            else:
                raise RPCError(ret['error']['message'])
        else:
            return ret["result"]

    def load_api_id(self, api):
        if (api not in self.api_ids):
            api_id = self.request('login', api, [])
            self.api_ids[api] = api_id
        return self.api_ids[api]

    def get_object(self, object_id):
        return self.request('database', 'get_objects', [[object_id]])[0]

    @cache.memoize()
    def get_global_properties(self):
        return self.request('database', 'get_global_properties', [])


class BitsharesWebsocketClientFactory():
    def get_instance(self, url = 1):
	if url == 1:
	    return BitsharesWebsocketClient(config.WEBSOCKET_URL)
	else:
	    return BitsharesWebsocketClient(config.WEBSOCKET_URL2)


import config

# client = BitsharesWebsocketClient(config.WEBSOCKET_URL)
client = BitsharesWebsocketClientFactory()
