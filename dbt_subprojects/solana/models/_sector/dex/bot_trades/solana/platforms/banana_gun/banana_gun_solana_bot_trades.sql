{{ config(
    alias = 'bot_trades',
    schema = 'banana_gun_solana',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = [
        'block_month',
        'blockchain',
        'tx_id',
        'tx_index',
        'outer_instruction_index',
        'inner_instruction_index'
    ]
   )
}}

{% set project_start_date = '2024-01-08' %}
{% set ci_start_date = '2026-08-11' %}
{% set ci_end_date = '2026-08-18' %}
{% set seed_start_date = '2024-03-23' %}
{% set seed_end_date = '2024-03-24' %}
{% set fee_receiver_1 = '8r2hZoDfk5hDWJ1sDujAi2Qr45ZyZw5EQxAXiMZWLKh2' %}
{% set fee_receiver_2 = 'Cj297UauzMX64FU9dKJZRUBWszJ7tEWpVheasq4CfATV' %}
{% set fee_receiver_3 = 'HKMh8nV3ysSofRi23LsfVGLGQKB415QAEfZT96kCcVj4' %}
{% set fee_receiver_4 = '7tQiiBdKoScWQkB1RmVuML7DBGnR31cuKPEtMM7Vy5SA' %}
{% set fee_receiver_5 = '4BBNEVRgrxVKv9f7pMNE788XM1tt379X9vNjpDH2KCL7' %}
{% set fee_receiver_6 = '47hEzz83VFR23rLTEeVm9A7eFzjJwjvdupPPmX3cePqF' %}
{% set fee_receiver_7 = 'EMbqD9Y9jLXEa3RbCR8AsEW1kVa3EiJgDLVgvKh4qNFP' %}
{% set fee_receiver_8 = 'Lk693UiTzQC4vobasRS1QGcYA9D6RGYLjHp1bWreQtM' %}

with bot_trades as (
    select
        matched_trades.block_time,
        matched_trades.block_date,
        matched_trades.block_month,
        matched_trades.blockchain,
        matched_trades.amount_usd,
        matched_trades.type,
        matched_trades.token_bought_amount,
        matched_trades.token_bought_symbol,
        matched_trades.token_bought_address,
        matched_trades.token_sold_amount,
        matched_trades.token_sold_symbol,
        matched_trades.token_sold_address,
        matched_trades.fee_usd,
        matched_trades.fee_token_amount,
        matched_trades.fee_token_symbol,
        matched_trades.fee_token_address,
        matched_trades.project,
        matched_trades.version,
        matched_trades.token_pair,
        matched_trades.project_contract_address,
        matched_trades.user,
        matched_trades.tx_id,
        matched_trades.tx_index,
        matched_trades.outer_instruction_index,
        matched_trades.inner_instruction_index
    from {{ source('solana', 'transactions') }} as transactions
    join {{ ref('banana_gun_solana_matched_trades') }} as matched_trades
        on transactions.id = matched_trades.tx_id
        and transactions.block_time = matched_trades.block_time
        and transactions.block_date = matched_trades.block_date
        {% if is_incremental() %}
            and {{ incremental_predicate('matched_trades.block_time') }}
        {% else %}
            {# Temporary bounded CI window plus the historical seed-test partition. #}
            and (
                (
                    matched_trades.block_month >= date_trunc(
                        'month', date '{{ ci_start_date }}'
                    )
                    and matched_trades.block_time >= timestamp '{{ ci_start_date }}'
                    and matched_trades.block_time < timestamp '{{ ci_end_date }}'
                )
                or (
                    matched_trades.block_month = date_trunc(
                        'month', date '{{ seed_start_date }}'
                    )
                    and matched_trades.block_time >= timestamp '{{ seed_start_date }}'
                    and matched_trades.block_time < timestamp '{{ seed_end_date }}'
                )
            )
        {% endif %}
    where
        transactions.signer not in (
            '{{ fee_receiver_1 }}',
            '{{ fee_receiver_2 }}',
            '{{ fee_receiver_3 }}',
            '{{ fee_receiver_4 }}',
            '{{ fee_receiver_5 }}',
            '{{ fee_receiver_6 }}',
            '{{ fee_receiver_7 }}',
            '{{ fee_receiver_8 }}'
        )
        {% if is_incremental() %}
            and {{ incremental_predicate('transactions.block_time') }}
        {% else %}
            {# Temporary bounded CI window plus the historical seed-test partition. #}
            and (
                (
                    transactions.block_date >= date '{{ ci_start_date }}'
                    and transactions.block_date < date '{{ ci_end_date }}'
                    and transactions.block_time >= timestamp '{{ ci_start_date }}'
                    and transactions.block_time < timestamp '{{ ci_end_date }}'
                )
                or (
                    transactions.block_date >= date '{{ seed_start_date }}'
                    and transactions.block_date < date '{{ seed_end_date }}'
                    and transactions.block_time >= timestamp '{{ seed_start_date }}'
                    and transactions.block_time < timestamp '{{ seed_end_date }}'
                )
            )
        {% endif %}
)

select
    block_time,
    block_date,
    block_month,
    'Banana Gun' as bot,
    blockchain,
    amount_usd,
    type,
    token_bought_amount,
    token_bought_symbol,
    token_bought_address,
    token_sold_amount,
    token_sold_symbol,
    token_sold_address,
    fee_usd,
    fee_token_amount,
    fee_token_symbol,
    fee_token_address,
    project,
    version,
    token_pair,
    project_contract_address,
    user,
    tx_id,
    tx_index,
    outer_instruction_index,
    coalesce(inner_instruction_index, 0) as inner_instruction_index,
    if(
        inner_instruction_index = max(inner_instruction_index) over (
            partition by tx_id, outer_instruction_index
        ),
        true,
        false
    ) as is_last_trade_in_transaction
from bot_trades
