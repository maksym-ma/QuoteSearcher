import urllib.parse

from google.cloud import storage


def list_bucket_objects(bucket_name):
    client = storage.Client()
    images = []
    for blob in client.list_blobs(bucket_name):
        images.append(f"https://storage.googleapis.com/{bucket_name}/{urllib.parse.quote_plus(blob.name)}")
    return images


def write_images_to_bucket(bucket_name, image, name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    # Download the image
    thumbnail_blob = bucket.blob(name)
    thumbnail_blob.upload_from_string(image)


def clean_bucket(bucket_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    # list all objects in the directory
    blobs = bucket.list_blobs()
    for blob in blobs:
        blob.delete()
