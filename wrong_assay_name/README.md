# Fixing Assays Name

## Query from RL Team

```sql
select *
from wet_collab_prd.analytics.rl_experimental_data_non_agg
where lower(assay_name) ilike '%enzymatic%' ;
```

## Data to retreive the wrong names in wet-collab Prod

```sql
select
    distinct split_part(res.synthesis_id, '_', 2) as project_name_results,
    syn.project_name as project_name_synthese,
    res.study_number,
    res.assay_name,
    res.metric_type,
    st.cro_name,
    st.ordered_at
from v1.assay_results res
left join v1.syntheses syn ON res.synthesis_id = syn.synthesis_id
left join v1.studies st on  res.study_number = st.study_number
where  assay_name ilike 'RL%'
order by split_part(res.synthesis_id, '_', 2) asc;
```

This query was used to create the CSV file that was mapped by the Drug Discovery Team. The resulting file was used to
update the assays names.

The update script is divided in 2 parts:

1. Insertion of the assays in the table: **assays**
2. Updating the assays names in the table: **assay_results**
