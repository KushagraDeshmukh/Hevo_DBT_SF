select * from {{ source('Snowflake', 'HEVO_RAW_PAYMENTS') }}

