select
    *
from {{ source('raw_cryptocurrencies_list', 'raw_coincap_currencies') }}
union all

select 
    *
from {{ this }}
where id not in (
    select id from {{ source('raw_cryptocurrencies_list', 'raw_coincap_currencies') }}
)