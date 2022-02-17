import io

from PIL import Image, ImageFont, ImageDraw
import time
import urllib.request
import image_edit.dominant_color as ie
import gcs.utils as gu
import bigquery.utils as bu
import random


def text_overlay(image_path, quotedata):

    urllib.request.urlretrieve(image_path, "tmp.jpg")

    domcol = ie.get_dom_color(image_path)

    my_image = Image.open("tmp.jpg")

    iw, ih = my_image.size

    title_font = ImageFont.truetype(
        'image_edit/fonts/Hahmlet-Medium.ttf',
        int(ih/(len(quotedata["quote"])/6.5))
    )
    title_text = quotedata["quote"]

    author_font = ImageFont.truetype(
        'image_edit/fonts/Hahmlet-Thin.ttf',
        int(ih/15)
    )
    author_text = quotedata["author"]

    image_editable = ImageDraw.Draw(my_image)
    image_editable.text(
        xy=(int(iw/2), int(ih/4)),
        text=title_text,
        fill=(int(domcol[0]), int(domcol[1]), int(domcol[2])),
        align="center",
        anchor="ms",
        spacing=-10,
        stroke_width=int(iw/200),
        stroke_fill=(
            int(domcol[0])-125 if int(domcol[0]) > 125 else int(domcol[0])+125,
            int(domcol[1])-125 if int(domcol[1]) > 125 else int(domcol[1])+125,
            int(domcol[2])-125 if int(domcol[2]) > 125 else int(domcol[2])+125,
            125
        ),
        font=title_font
    )

    image_editable.text(
        xy=(int(iw/2), int(3*ih/4)),
        text=author_text,
        fill=(int(domcol[0]), int(domcol[1]), int(domcol[2])),
        align="center",
        anchor="ms",
        spacing=-10,
        stroke_width=int(iw/400),
        stroke_fill=(
            int(domcol[0])-125 if int(domcol[0]) > 125 else int(domcol[0])+125,
            int(domcol[1])-125 if int(domcol[1]) > 125 else int(domcol[1])+125,
            int(domcol[2])-125 if int(domcol[2]) > 125 else int(domcol[2])+125
        ),
        font=author_font
    )

    img_byte_arr = io.BytesIO()
    my_image.save('result.jpg')
    my_image.save(img_byte_arr, format='jpeg')
    img_byte_arr = img_byte_arr.getvalue()
    gu.write_images_to_bucket("images_ready_to_post", img_byte_arr, f"topost_{random.randint(1, 100)}.jpg")


images = gu.list_bucket_objects("images_clean_source")
quotedata = bu.get_random_quote()
text_overlay(images[random.randint(0, len(images)-1)], quotedata)
