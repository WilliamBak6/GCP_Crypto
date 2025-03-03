select
    priceUsd,
    round(cast(time as float64))                            as time,
    date,
    id

from {{ source('raw_cryptocurrencies_list', 'raw_coincap_prices') }}
union all 

select 
    *
from {{ this }}
where date not in (
    select date from {{ source('raw_cryptocurrencies_list', 'raw_coincap_prices') }}
)