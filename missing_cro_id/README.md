# Retrieving missing CRO_ID

Historical data stored in snowflake

```sql
select
    e.cro_shipped_id,
    t.ordered_id,
    e.order_motive,
    e.project_name
from wet_collab_prd.analytics.rl_experimental_data_non_agg e
left join experimental_data_ingestion_prd.analytics.rl_order_trackings_v3 t
    on e.cro_shipped_id = t.shipped_id
where e.cro_ordered_id is null;
```
