import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DSN = os.getenv("DSN")
CSV_PATH = "missing_cro_id.csv"

UPDATE_SQL = """
    UPDATE v1.compound_orders co
    SET cro_id = u.new_cro_id
    FROM tmp_cro_updates u
    JOIN v1.synonym syno
        ON syno.synonym = u.cro_shipped_id
    JOIN v1.in_vitro_compounds ivc
        ON ivc.in_vitro_compound_id = syno.in_vitro_compound_id
    JOIN v1.syntheses syn
        ON syn.canonical_cx_smiles = ivc.canonical_cx_smiles
    WHERE
        syn.order_id = co.order_id
        AND co.cro_name = 'Enamine'
        AND co.cro_id IS NULL
        AND co.project_name = u.project_name
        AND co.order_tags ->> 'rl_order_motive' = u.order_motive;
"""

conn = psycopg2.connect(DSN)

with conn:
    with conn.cursor() as cur:
        
        # Creates an staging table to store data from CSV
        cur.execute("""
            CREATE TEMP TABLE tmp_cro_updates (
                cro_shipped_id text,
                new_cro_id     text,
                order_motive   text,
                project_name   text
            ) ON COMMIT DROP;
        """)

        # Load data to the staging table
        with open(CSV_PATH, "r", encoding="utf-8") as f:
            cur.copy_expert("""
                COPY tmp_cro_updates (
                            cro_shipped_id,
                            new_cro_id,
                            order_motive,
                            project_name
                            )
                FROM STDIN WITH (FORMAT csv, HEADER true)
            """, f)

        # Update table: **compound_orders**
        cur.execute(UPDATE_SQL)
        print(f"Updated {cur.rowcount} rows in v1.compound_orders")

conn.close()