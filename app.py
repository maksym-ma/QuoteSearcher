import os

from flask import Flask
from flask_restful import Api
import routing as r

app = Flask(__name__)
api = Api(app)

r.init_routing(api, app)

if __name__ == '__main__':
    server_port = os.environ.get('PORT', '8080')
    app.run(debug=True, port=server_port, host='0.0.0.0')
