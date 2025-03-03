with _ingestion as (
    select 
        *
    from {{ ref('load_coincap_prices') }}
),

_model1 as (
    select
        * except (date, time),
        date(replace(_i.date, "T00:00:00.000Z", ""))   as date

    from _ingestion _i
)

select * from _model1