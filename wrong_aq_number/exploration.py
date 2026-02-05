import duckdb
import pandas as pd
from rdkit import Chem

with Chem.SDMolSupplier("2026_01_30_nek2_no aggregation_assay_results_dashboard.sdf") as mols:
    props = [mol.GetPropsAsDict(includePrivate=True) for mol in mols]
    df_jan = pd.DataFrame(props)

with Chem.SDMolSupplier("2026_02_05_nek2_no aggregation_assay_results_dashboard.sdf") as mols:
    props = [mol.GetPropsAsDict(includePrivate=True) for mol in mols]
    df_fev = pd.DataFrame(props)


query = """
    SELECT
        jan.compound_id as compound_id_january,
        fev.compound_id as compound_id_february,

        jan.synthesis_id as synthesis_id_january,
        fev.synthesis_id as synthesis_id_february,

        jan.synonyms as synonyms_january,
        fev.synonyms as synonyms_february,

        jan.synthesis_order_date as synthesis_order_date_january,
        fev.synthesis_order_date as synthesis_order_date_february,

        right(jan.compound_id, 5) as molecule_id_january,
        right(fev.compound_id, 5) as molecule_id_february
    FROM
        df_jan jan
    INNER JOIN df_fev fev ON jan.synonyms = fev.synonyms
    WHERE jan.compound_id <> fev.compound_id
"""
df = duckdb.query(query).write_csv("not_matching_compounds_id.csv")

print(df)
