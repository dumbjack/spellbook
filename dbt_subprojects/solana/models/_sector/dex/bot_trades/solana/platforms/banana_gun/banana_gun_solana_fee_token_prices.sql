{{ config(
    schema = 'banana_gun_solana',
    alias = 'fee_token_prices',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.minute')],
    unique_key = ['block_month', 'minute', 'contract_address_varbinary']
   )
}}

{% set project_start_date = '2024-01-08' %}
{% set ci_start_date = '2026-08-11' %}
{% set ci_end_date = '2026-08-18' %}
{% set seed_start_date = '2024-03-23' %}
{% set seed_end_date = '2024-03-24' %}

with fee_payments as (
    select
        *,
        date_trunc('minute', block_time) as minute,
        from_base58(token_address) as contract_address_varbinary,
        token_address as contract_address_base58
    from {{ ref('banana_gun_solana_fee_payments_raw') }}
    {% if is_incremental() %}
        where {{ incremental_predicate('block_time') }}
    {% else %}
        {# Temporary bounded CI window plus the historical seed-test partition. #}
        where (
            (
                block_month >= date_trunc('month', date '{{ ci_start_date }}')
                and block_time >= timestamp '{{ ci_start_date }}'
                and block_time < timestamp '{{ ci_end_date }}'
            )
            or (
                block_month = date_trunc('month', date '{{ seed_start_date }}')
                and block_time >= timestamp '{{ seed_start_date }}'
                and block_time < timestamp '{{ seed_end_date }}'
            )
        )
    {% endif %}
),

distinct_fee_tokens_per_minute as (
    select distinct
        contract_address_varbinary,
        contract_address_base58,
        blockchain,
        minute
    from fee_payments
)

select
    tokens.minute,
    cast(date_trunc('month', tokens.minute) as date) as block_month,
    tokens.blockchain,
    prices.symbol,
    prices.price,
    prices.decimals,
    tokens.contract_address_varbinary,
    tokens.contract_address_base58
from distinct_fee_tokens_per_minute as tokens
join {{ source('prices', 'usd') }} as prices
    on prices.blockchain = tokens.blockchain
    and prices.contract_address = tokens.contract_address_varbinary
    and prices.minute = tokens.minute
    {% if is_incremental() %}
        and {{ incremental_predicate('prices.minute') }}
    {% else %}
        {# Temporary bounded CI window plus the historical seed-test partition. #}
        and (
            (
                prices.minute >= timestamp '{{ ci_start_date }}'
                and prices.minute < timestamp '{{ ci_end_date }}'
            )
            or (
                prices.minute >= timestamp '{{ seed_start_date }}'
                and prices.minute < timestamp '{{ seed_end_date }}'
            )
        )
    {% endif %}
