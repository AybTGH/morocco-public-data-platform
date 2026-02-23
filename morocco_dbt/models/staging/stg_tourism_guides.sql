with source as (
    select * from {{ source('staging', 'tourism_guides') }}
),

cleaned as (
    select
        nullif(trim(nom), '') as last_name,
        nullif(trim(prenom), '') as first_name,
        nullif(trim(ville), '') as city,
        nullif(trim(categorie), '') as category,
        nullif(trim(langue_de_travail), '') as work_language
    from source
)

select * from cleaned