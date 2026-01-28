import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DSN = os.getenv("DSN")
conn = psycopg2.connect(DSN)


CSV_PATH = "corrected_names.csv"

INSERT_ASSAY_SQL = """
  INSERT INTO v1.assays (
      name,
      type,
      cro_name,
      metadata,
      created_at,
      updated_at,
      parser_name,
      classification
  )
  SELECT
      u.correct_assay_name AS name,
      'other' AS type,
      MIN(u.cro_name) AS cro_name,
      '{}'::json AS metadata,
      now() AS created_at,
      now() AS updated_at,
      'manual_insertion' AS parser_name,
      'Pharmacology in vitro - Enzymatic' AS classification
  FROM tmp_cro_updates u
  LEFT JOIN v1.assays a
  ON a.name = u.correct_assay_name
  WHERE a.name IS NULL
  GROUP BY u.correct_assay_name;
"""


UPDATE_SQL = """
   UPDATE v1.assay_results res
    SET assay_name = u.correct_assay_name
    FROM tmp_cro_updates u
    JOIN v1.syntheses syn
        ON syn.project_name = u.project_name_synthese
    JOIN v1.studies st
        ON st.study_number::text = u.study_number::text
    WHERE
        res.assay_name ILIKE 'RL%'
        AND res.synthesis_id = syn.synthesis_id
        AND u.project_name_results = split_part(res.synthesis_id, '_', 2)
        AND res.study_number::text = u.study_number::text
        AND res.assay_name = u.assay_name
        AND res.metric_type = u.metric_type
        AND st.cro_name = u.cro_name
        AND to_timestamp(u.ordered_at, 'DD/MM/YYYY HH24:MI')::date = st.ordered_at::date
        AND res.assay_name IS DISTINCT FROM u.correct_assay_name;
    """


with conn:
    with conn.cursor() as cur:
        # Creates an staging table to store data from CSV
        cur.execute("""
            CREATE TEMP TABLE tmp_cro_updates (
                project_name_results text,
                project_name_synthese text,
                study_number text,
                assay_name text,
                correct_assay_name text,
                metric_type text,
                cro_name text,
                ordered_at text
            ) ON COMMIT DROP;
        """)

        # Load data to the staging table
        with open(CSV_PATH, "r", encoding="utf-8") as f:
            cur.copy_expert(
                """
                COPY tmp_cro_updates (
                            project_name_results,
                            project_name_synthese,
                            study_number,
                            assay_name,
                            correct_assay_name,
                            metric_type,
                            cro_name,
                            ordered_at
                            )
                FROM STDIN WITH (FORMAT csv, HEADER true)
            """,
                f,
            )

        # Create the missing assays in table: **assays**
        cur.execute(INSERT_ASSAY_SQL)
        print(f"Updated {cur.rowcount} rows in v1.assays")

        # Update table: **assay_results**
        cur.execute(UPDATE_SQL)
        print(f"Updated {cur.rowcount} rows in v1.assay_results")

conn.close()
