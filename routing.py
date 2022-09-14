import random
import sys
import time
import logging
import traceback

import bigquery.utils as bu
import gcs.utils as gu
import image_edit.utils as ieu
import instagram.client as ic


def init_routing(api, app):
    @app.route('/publish', methods=['post', 'get'])
    def front_form():
        #time.sleep(random.randint(60, 300))
        logging.info("publish initiated")
        print("publish initiated")
        images = gu.list_bucket_objects("images_clean_source")

        for x in range(0, 2):
            logging.info(f"Image iteration {x}")
            time.sleep(10)
            # quotedata = bu.get_random_quote()
            # ieu.text_overlay(images[random.randint(0, len(images) - 1)], quotedata)

            try:
                quotedata = bu.get_random_quote()
                ieu.text_overlay(images[random.randint(0, len(images) - 1)], quotedata)
                logging.info("Image generated")
            except Exception as e:
                logging.info("Image generation failed")
                logging.error(f"{sys.exc_info()} -> {traceback.format_exc(e)}")

        insta_client = ic.Instagram()

        images_to_post = gu.list_bucket_objects("images_ready_to_post")
        for im in images_to_post:
            time.sleep(random.randint(15, 30))
            media = insta_client.create_media(im)

            insta_client.post_media(media)
            logging.info("Image posted")

        gu.clean_bucket("images_ready_to_post")
        logging.info("Bucket cleaned")
        return "published"

    @app.route('/', methods=['post', 'get'])
    def main():
        return "main page"
