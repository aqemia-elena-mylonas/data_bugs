import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DSN = os.getenv("DSN")

UPDATE_SQL = """
    UPDATE v1.syntheses syn
    SET delivery_date = ord.order_date
    FROM v1.compound_orders ord
    WHERE syn.order_id = ord.order_id
    AND syn.delivery_date IS NULL
    AND syn.cro_name = 'Enamine';
"""

conn = psycopg2.connect(DSN)

with conn:
    with conn.cursor() as cur:
        cur.execute(UPDATE_SQL)
        print(f"Updated {cur.rowcount} rows in v1.assay_results")

conn.close()
