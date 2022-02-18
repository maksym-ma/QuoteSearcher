import re
import textwrap

from google.cloud import bigquery


def get_random_quote():
    client = bigquery.Client()
    query_job = client.query(
        """
            SELECT
                *
            FROM
              `quotesearcher.datasamples.quotes`
            WHERE LENGTH(Quote) BETWEEN 50 AND 90
            AND Category != "quotes"
            AND Popularity > 0
            AND LENGTH(Author) BETWEEN 10 AND 50
            ORDER BY RAND()
            LIMIT 1 
        """
    )

    results = query_job.result()  # Waits for job to complete.
    quote_data = {
        "quote": "",
        "author": "",
        "tags": [""]
    }
    for row in results:
        quote_data["quote"] = '\n'.join(textwrap.wrap(re.sub(' +', ' ', row.Quote).strip(" "), 35))
        quote_data["author"] = re.sub(' +', ' ', row.Author).strip(" ")
        quote_data["tags"] = "#" + "#".join([re.sub(' +', ' ', i).strip(" ") for i in list(set(row.Tags))[:5]]).lower()

    return quote_data
