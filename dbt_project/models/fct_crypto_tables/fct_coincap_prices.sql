with _t1 as (
    select
        * except (priceUsd),
        cast(priceUsd as float64)                   as priceUsd

    from {{ ref('int_coincap_prices') }}
),

_t2 as (
    select
        id,
        priceUsd,
        date,
        case
            when extract(year from date) = extract(year from current_date()) then 'y0'
            when extract(year from date) = extract(year from current_date()) - 1 then 'y1'
            when extract(year from date) = extract(year from current_date()) - 2 then 'y2'
            when extract(year from date) = extract(year from current_date()) - 3 then 'y3'
            else NULL
        end                                                                                     as year,

    from _t1
),

_pivoted as (
    select 
        *
    from _t2
    pivot(
        sum(priceUsd) for year in ('y0', 'y1', 'y2', 'y3')
    )
),

_t3 as (
    select
        * except (date),
        case 
            when extract(year from date) = extract(year from current_date()) - 1 then date_add(date, interval 1 year)
            when extract(year from date) = extract(year from current_date()) - 2 then date_add(date, interval 2 year)
            when extract(year from date) = extract(year from current_date()) - 3 then date_add(date, interval 3 year)
            when extract(year from date) = extract(year from current_date()) then date
            else NULL
        end                                                                                                                 as date

    from _pivoted   
),

_gross as (
    select
        id,
        date,
        sum(y0)                                 as y0,
        sum(y1)                                 as y1,
        sum(y2)                                 as y2

    from _t3
    group by id, date
),

_final as (
    select
        *,
        ((sum(y0) - sum(y1)) / sum(y1)) * 100 as YoY1,
        ((sum(y0) - sum(y2)) / sum(y2)) * 100 as YoY2
    
    from _gross
    group by all
)

select * from _final
