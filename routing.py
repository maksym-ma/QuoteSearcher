import datetime
import sys
import time
import os
import requests
import json
from urllib.parse import unquote
import gcs.utils as gu
import bigquery.utils as bu
import image_edit.utils as ieu
import instagram.client as ic
import random
import sys
import time

from flask import request
import logging


def init_routing(api, app):

    @app.route('/publish', methods=['post', 'get'])
    def front_form():
        time.sleep(random.randint(60, 300))

        images = gu.list_bucket_objects("images_clean_source")

        for x in range(0, 1):
            # print(x)
            time.sleep(10)
            # quotedata = bu.get_random_quote()
            # ieu.text_overlay(images[random.randint(0, len(images) - 1)], quotedata)

            try:
                quotedata = bu.get_random_quote()
                ieu.text_overlay(images[random.randint(0, len(images) - 1)], quotedata)
            except:
                print(sys.exc_info())

        insta_client = ic.Instagram()

        images_to_post = gu.list_bucket_objects("images_ready_to_post")
        for im in images_to_post:
            time.sleep(random.randint(60, 75))
            print(im)
            media = insta_client.create_media(im)
            print(media)
            insta_client.post_media(media)

        gu.clean_bucket("images_ready_to_post")
        return "published"
