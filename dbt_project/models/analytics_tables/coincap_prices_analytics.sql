with _t1 as (
    select 
        *
    from {{ ref('fct_coincap_prices') }}
),

_t2 as (
    select
        id as nameid,
        rank,
        symbol,
        changePercent24H as last_registered_daily_change_percent

    from {{ ref('dim_coincap_currencies') }}
),

_jointure as (
    select 
        *
    from _t1 
    left join _t2 on _t1.id = _t2.nameid
)

select * from _jointure