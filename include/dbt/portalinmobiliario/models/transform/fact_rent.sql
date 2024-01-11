-- Create the fact table by joining the relevant keys from dimension table
WITH latest_uf_value AS (
    SELECT value
    FROM {{ source('portalinmobiliario', 'uf_values')}}
    ORDER BY date DESC
    LIMIT 1
),
fct_rent_cte AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['direccion', 'precio', 'dormitorios', 'superficie']) }} as apartment_id,
        CASE
            WHEN precio < 100 THEN precio * CAST(ROUND(CAST((SELECT value FROM latest_uf_value) AS float64), 0) as int)
            ELSE precio
        END
        AS price,
        superficie AS surface,
        dormitorios AS rooms
    FROM {{ source('portalinmobiliario', 'apartments_rm') }}
)
SELECT
    fct.apartment_id,
    fct.price,
    fct.surface,
    fct.rooms
FROM
    fct_rent_cte AS fct 
INNER JOIN
    {{ ref('dim_apartment') }} AS da ON fct.apartment_id = da.apartment_id
