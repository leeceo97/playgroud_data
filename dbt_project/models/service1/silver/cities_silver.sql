{{ config(schema='silver')}}
-- trim and uppercase city
-- remove '*'
-- remove rows where city is empty
-- remove rows where city is in specified list
SELECT
    distinct trim(upper(
        replace(c1, '*', '')
    )) as city
FROM {{ ref('cities_bronze') }}
where c1 is not null
and not c1 in ('England', 'Scotland', 'Wales', 'Northern Ireland')