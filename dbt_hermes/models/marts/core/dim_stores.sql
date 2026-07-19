SELECT  
    {{ dbt_utils.generate_surrogate_key(['store_id']) }} AS store_key,
    store_id,
    store_name,
    city,
    region,
    postal_code,
    store_format,
    opening_date,
    is_active
FROM {{ ref('stg_stores') }}