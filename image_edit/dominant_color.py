from __future__ import print_function

import urllib.request

import numpy as np
import scipy
import scipy.cluster
import scipy.misc
from PIL import Image


def get_dom_color(image_path):
    clusters = 5
    urllib.request.urlretrieve(image_path, "tmp.jpg")

    im = Image.open("tmp.jpg")
    im = im.resize((150, 150))  # optional, to reduce time
    ar = np.asarray(im)
    shape = ar.shape
    ar = ar.reshape(scipy.product(shape[:2]), shape[2]).astype(float)

    codes, dist = scipy.cluster.vq.kmeans(ar, clusters)

    vecs, dist = scipy.cluster.vq.vq(ar, codes)
    counts, bins = scipy.histogram(vecs, len(codes))

    index_max = scipy.argmax(counts)
    peak = codes[index_max]
    return peak
