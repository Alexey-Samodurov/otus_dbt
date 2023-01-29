{{
   config(
     materialized = "table",
     engine = "MergeTree()",
     order_by = "year"
   )
}}

SELECT
    sum(LO_REVENUE),
    toYear(LO_ORDERDATE) AS year,
    P_BRAND
FROM {{ source('dbgen', 'lineorder_flat') }}
WHERE P_CATEGORY = 'MFGR#12' AND S_REGION = 'AMERICA'
GROUP BY
    year,
    P_BRAND
ORDER BY
    year,
    P_BRAND