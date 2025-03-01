with _ingestion as (
    select 
        * except (explorer, supply, maxSupply)
    
    from {{ ref('load_coincap_currencies') }}
),

_model1 as (
    select 
        * except (marketCapUSD, volumeUsd24h),
        cast(marketCapUSD as float64)                                   as marketCapUSD,
        cast(volumeUsd24h as float64)                                   as last_day_volumeUsd

    from _ingestion
)

select * from _model1