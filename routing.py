import datetime
import sys
import time

from flask import request
import bigquery.service as bs
import binance_parser.controller as bps
import binance_parser.service as bpser
import logging


def init_routing(api, app):

    @app.route('/', methods=['post', 'get'])
    def front_form():
        return "empty"
