-- Create the aparment dimension table
WITH apartment_cte AS (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['direccion', 'precio', 'dormitorios', 'superficie']) }} as apartment_id,
        direccion AS address,
        -- When "comuna" is equal to "Chile" then use "Zona" as comuna
        -- In this cases, the commune is wrongly stored in "zona"
        CASE
            WHEN comuna = 'Chile' THEN zona
            ELSE comuna
        END
        AS comuna,
        zona AS area
    FROM
        {{ source('portalinmobiliario', 'apartments_rm') }}
)
SELECT
    a_cte.*
FROM
    apartment_cte AS a_cte