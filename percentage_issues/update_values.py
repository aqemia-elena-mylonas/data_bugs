import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DSN = os.getenv("DSN")

UPDATE_SQL = """
    UPDATE v1.assay_results
    SET value = value * 100
    WHERE
    assay_name IN (
        'OATP1B1-transporter inhibition (Pharmaron)',
        'BSEP inhibition assay (Pharmaron)'
    )
    AND metric_type ILIKE '%perc%';
"""

conn = psycopg2.connect(DSN)

with conn:
    with conn.cursor() as cur:
        cur.execute(UPDATE_SQL)
        print(f"Updated {cur.rowcount} rows in v1.assay_results")

conn.close()
