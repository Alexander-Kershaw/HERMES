SELECT  
    {{ dbt.utils.generate_surrogate_key(['customer_id']) }} AS customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    city,
    region,
    postal_code,
    address,
    loyalty_tier,
    creation_datetime,
    is_active
FROM {{ REF('stg_customers') }}