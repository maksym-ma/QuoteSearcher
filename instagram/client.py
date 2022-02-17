import os
import requests
import json

if not os.environ.get("access_token"):
    import config


class Instagram:
    access_token = os.environ.get("access_token") if os.environ.get("access_token") else config.access_token
    base_url = "https://graph.facebook.com/v13.0/"
    insta_account_id = "17841452169613325"
    print(access_token)

    def __init__(self):
        pass

    def create_media(self, image_location=None, caption=None):

        if image_location is None:
            image_location = 'https://storage.googleapis.com/images_manual/5h63wk.jpg'
        post_url = '{}{}/media'.format(self.base_url, self.insta_account_id)

        payload = {
            'image_url': 'https://storage.googleapis.com/images_manual/5h63wk.jpg' if image_location is None else image_location,
            'caption': 'Testing some API stuff' if caption is None else caption,
            'access_token': self.access_token
        }
        r = requests.post(post_url, data=payload)
        return r.text

    def post_media(self, media_data=None):

        result = json.loads(media_data)
        if 'id' in result:
            creation_id = result['id']
            post_url = '{}{}/media_publish'.format(self.base_url, self.insta_account_id)
            payload = {
                'creation_id': creation_id,
                'access_token': self.access_token
            }
            r = requests.post(post_url, data=payload)
            print('--------Just posted to Instagram--------')
            print(r.text)
        else:
            print('Created media could not be parsed')


insta_client = Instagram()
media = insta_client.create_media()
print(media)
insta_client.post_media(media)
