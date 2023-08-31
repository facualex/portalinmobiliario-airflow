-- Create the aparment dimension table
WITH apartment_cte AS (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['direccion', 'precio', 'dormitorios', 'superficie']) }} as apartment_id,
        direccion AS address,
        zona AS area,
        comuna AS commune
    FROM
        {{ source('portalinmobiliario', 'apartments_rm') }}
)
SELECT
    a_cte.*
FROM
    apartment_cte AS a_cte