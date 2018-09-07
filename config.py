import os


# WEBSOCKET_URL = os.environ.get('WEBSOCKET_URL', "ws://localhost:8090/ws")
WEBSOCKET_URL = os.environ.get('WEBSOCKET_URL', "wss://shanghai.51nebula.com/")
WEBSOCKET_URL2 = os.environ.get('WEBSOCKET_URL', "ws://121.40.95.24:8090/")
# a connection to a bitshares full node
# FULL_WEBSOCKET_URL = os.environ.get('FULL_WEBSOCKET_URL', "ws://88.99.145.10:9999/ws")
FULL_WEBSOCKET_URL = os.environ.get('FULL_WEBSOCKET_URL', "wss://shanghai.51nebula.com/")

# a connection to an ElasticSearch wrapper
#ES_WRAPPER = os.environ.get('ES_WRAPPER', "http://185.208.208.184:5000") # oxarbitrage
ES_WRAPPER = os.environ.get('ES_WRAPPER', "http://95.216.32.252:5000") # clockwork
# MONGODB_DB_URL = os.environ.get('MONGO_WRAPPER', "mongodb://yoyo:yoyo123@39.105.55.115:27017/cybex") # clockwork
MONGODB_DB_URL = os.environ.get('MONGO_WRAPPER', "mongodb://monitor:ka649Ndhy10&@47.91.216.172:27017/cybex") # clockwork
MONGODB_DB_NAME = os.environ.get('MONGO_DB_NAME', 'cybex')
#ES_WRAPPER = os.environ.get('ES_WRAPPER', "https://eswrapper.bitshares.eu") # Infrastructure worker

# Database connection: see https://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-PARAMKEYWORDS
POSTGRES = {'host': os.environ.get('POSTGRES_HOST', 'localhost'),
            'port': os.environ.get('POSTGRES_PORT', '5432'),
            'database': os.environ.get('POSTGRES_DATABASE', 'explorer'),
            'user': os.environ.get('POSTGRES_USER', 'postgres'),
            'password': os.environ.get('POSTGRES_PASSWORD', 'posta'),
}

# Cache: see https://flask-caching.readthedocs.io/en/latest/#configuring-flask-caching
CACHE = {
    'CACHE_TYPE': os.environ.get('CACHE_TYPE', 'simple'),
    'CACHE_DEFAULT_TIMEOUT': int(os.environ.get('CACHE_DEFAULT_TIMEOUT', 600)) # 10 min
}

# Configure profiler: see https://github.com/muatik/flask-profiler
PROFILER = {
    'enabled': os.environ.get('PROFILER_ENABLED', False),
    'username': os.environ.get('PROFILER_USERNAME', None),
    'password': os.environ.get('PROFILER_PASSWORD', None),
}

# CORE_ASSET_SYMBOL = 'BTS'
CORE_ASSET_SYMBOL = 'CYB'
CORE_ASSET_ID = '1.3.0'

TESTNET = 0 # 0 = not in the testnet, 1 = testnet
CORE_ASSET_SYMBOL_TESTNET = 'TEST'
