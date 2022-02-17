from google.cloud import storage


def list_bucket_objects(bucket_name):
    client = storage.Client()
    images = []
    for blob in client.list_blobs(bucket_name):
        images.append(f"https://storage.googleapis.com/{bucket_name}/{blob.name}")
    return images


def write_images_to_bucket(bucket_name, image, name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    thumbnail_blob = bucket.blob(name)
    thumbnail_blob.upload_from_file(image)
    return 1
