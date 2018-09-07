# prepare

1. install all the dependecies according to official version: https://github.com/oxarbitrage/bitshares-explorer-api

2. for cybex version, only python and npm part is needed.
besides, you also need to install pymongo lib becuase cybex explore backend use mongo to store ops rather than elasticsearch, this is a big difference.
3. cybex explorer remove postgres db part ant related apis, so when you install the deps, you can ignore that part.

4. URL set config.py
```
import os


WEBSOCKET_URL = os.environ.get('WEBSOCKET_URL', "wss://shanghai.51nebula.com/")
# WEBSOCKET_URL2 = os.environ.get('WEBSOCKET_URL', "ws://121.40.95.24:8090/")
WEBSOCKET_URL2 = os.environ.get('WEBSOCKET_URL', "ws://47.91.216.172:8090/")
FULL_WEBSOCKET_URL = os.environ.get('FULL_WEBSOCKET_URL', "wss://shanghai.51nebula.com/")

MONGODB_DB_URL = os.environ.get('MONGO_WRAPPER', "mongodb://monitor:ka649Ndhy10&@47.91.216.172:27017/cybex") # clockwork
MONGODB_DB_NAME = os.environ.get('MONGO_DB_NAME', 'cybex')


```


# run

run 
```
# cd <to your path>
# ./start.sh > log.log 2>&1 &
```
check with logs or use ps -ef| grep flask to check your flask service.


start.sh use 8081 as default, you can change it.

# check restful api

http://47.91.216.172:8081/apidocs


