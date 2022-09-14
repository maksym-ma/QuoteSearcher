import random
import sys
import time
import logging

import bigquery.utils as bu
import gcs.utils as gu
import image_edit.utils as ieu
import instagram.client as ic


def init_routing(api, app):
    @app.route('/publish', methods=['post', 'get'])
    def front_form():

        import sys
        sys.stdout.flush()
        print("publish initiated")
        images = gu.list_bucket_objects("images_clean_source")

        for x in range(0, 2):
            time.sleep(10)
            try:
                quotedata = bu.get_random_quote()
                print(quotedata)
                ieu.text_overlay(images[random.randint(0, len(images) - 1)], quotedata)
                print("Image  generated")
            except Exception as e:
                print(sys.exc_info())
                raise e

        insta_client = ic.Instagram()
        print("Client created")
        images_to_post = gu.list_bucket_objects("images_ready_to_post")
        for im in images_to_post:
            time.sleep(random.randint(15, 30))
            media = insta_client.create_media(im)
            print(f"Media created {media}")
            insta_client.post_media(media)
            print("Image posted")

        gu.clean_bucket("images_ready_to_post")
        print("Bucket cleaned")

        import sys
        sys.stdout.flush()
        return "published"
