import connexion

options = {'swagger_url': '/apidocs'}
app = connexion.FlaskApp('bitshares-explorer-api', options=options)

from flask_cors import CORS
CORS(app.app)

from services.cache import cache
cache.init_app(app.app)

app.add_api('api.yaml')

import services.profiler
services.profiler.init_app(app.app)

application = app.app

if __name__ == "__main__":
    print "port is 8081"
    # app.run(host='0.0.0.0', port = 8081, debug= True, use_reloader=True)
    app.run(host='0.0.0.0', port = 8081, use_reloader=True)


