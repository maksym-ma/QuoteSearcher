import io
import urllib.request

from PIL import Image, ImageFont, ImageDraw

import gcs.utils as gu
import image_edit.dominant_color as ie


def text_overlay(image_path, quotedata):
    urllib.request.urlretrieve(image_path, "tmp.jpg")

    domcol = ie.get_dom_color(image_path)

    print(sum(domcol))

    if sum(domcol) > 765 / 2:
        tr = 2
    else:
        tr = 255

    my_image = Image.open("tmp.jpg")
    my_image = my_image.convert("RGBA")

    iw, ih = my_image.size

    llx, lly = 0, int(ih / 7)
    urx, ury = iw + 1, int(ih / 1.95)

    lx, ly = 0, int(ih / 1.3)
    ux, uy = iw + 1, int(ih / 1.1)

    overlay = Image.new('RGBA', my_image.size, (tr, tr, tr) + (0,))
    draw = ImageDraw.Draw(overlay)  # Create a context for drawing things on it.
    draw.rectangle(((llx, lly), (urx, ury)), fill=(tr, tr, tr) + (80,))
    draw.rectangle(((lx, ly), (ux, uy)), fill=(tr, tr, tr) + (50git,))

    my_image = Image.alpha_composite(my_image, overlay)

    title_font = ImageFont.truetype(
        'image_edit/fonts/Hahmlet-Medium.ttf',
        int(ih / 12)
    )
    title_text = quotedata["quote"]

    author_font = ImageFont.truetype(
        'image_edit/fonts/Hahmlet-Regular.ttf',
        int(ih / 14)
    )
    author_text = quotedata["author"]

    image_editable = ImageDraw.Draw(my_image)
    image_editable.text(
        xy=(int(iw / 2), int(ih / 4)),
        text=title_text,
        fill=(int(domcol[0]), int(domcol[1]), int(domcol[2])),
        align="center",
        anchor="ms",
        spacing=-10,
        stroke_width=int(iw / 200),
        stroke_fill=(
            int(domcol[0]) - 110 if int(domcol[0]) > 125 else int(domcol[0]) + 140,
            int(domcol[1]) - 110 if int(domcol[1]) > 125 else int(domcol[1]) + 140,
            int(domcol[2]) - 110 if int(domcol[2]) > 125 else int(domcol[2]) + 140
        ),
        font=title_font
    )

    image_editable.text(
        xy=(int(iw / 2), int(ih / 1.3) - ((ih / 1.3) - (ih / 1.1)) / 2 + int(ih / 13.5) / 2),
        text=author_text,
        fill=(int(domcol[0]), int(domcol[1]), int(domcol[2])),
        align="center",
        anchor="ms",
        spacing=-10,
        stroke_width=int(iw / 600),
        stroke_fill=(
            int(domcol[0]) - 110 if int(domcol[0]) > 125 else int(domcol[0]) + 140,
            int(domcol[1]) - 110 if int(domcol[1]) > 125 else int(domcol[1]) + 140,
            int(domcol[2]) - 110 if int(domcol[2]) > 125 else int(domcol[2]) + 140
        ),
        font=author_font
    )

    my_image = my_image.convert("RGB")
    #my_image.save('result.jpg')

    img_byte_arr = io.BytesIO()
    my_image.save(img_byte_arr, format='jpeg')
    img_byte_arr = img_byte_arr.getvalue()
    gu.write_images_to_bucket("images_ready_to_post", img_byte_arr, f"{quotedata['tags']}.jpg")
