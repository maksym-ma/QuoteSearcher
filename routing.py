import datetime
import sys
import time

from flask import request
import logging


def init_routing(api, app):

    @app.route('/', methods=['post', 'get'])
    def front_form():
        return "empty"
