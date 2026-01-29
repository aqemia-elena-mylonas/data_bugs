import os
from datetime import date

import duckdb
from dotenv import load_dotenv

load_dotenv()

today = date.today()


def create_connection():
    """Create duckdb connection to postgres"""
    con = duckdb.connect()
    con.sql(f"""
        INSTALL postgres;
        LOAD postgres;
        ATTACH '
            dbname={os.getenv("POSTGRES_DB")}
            host={os.getenv("POSTGRES_HOST")}
            port={os.getenv("POSTGRES_PORT")}
            user={os.getenv("POSTGRES_USER")}
            password={os.getenv("POSTGRES_PASSWORD")}
        ' AS db (TYPE postgres, READ_ONLY);
    """)
    return con


con = create_connection()


con.sql(
    """
        select
            assay_name,
            metric_type,
            avg(value) as average,
            min(value) as minimum,
            max(value) as maximum
        from
            db.v1.assay_results
        where
            value_status='provided'
            and metric_type ilike '%perc%'
    group by assay_name, metric_type
    order by max(value)asc
"""
).write_csv(f"{str(today)}_assays_in_percentage.csv")
