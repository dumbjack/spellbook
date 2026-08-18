-- Compare the legacy transactions.signer exclusion with account_activity.signed
-- for the fixed 7-day and 30-day windows in the Banana Gun CI build.
--
-- Before running, replace <ci_schema> with the 30-day CI schema printed by
-- dbt CI from fork, for example:
-- dune_spellbook_ci__tmp_pr9945_<run_id>_<attempt>

with params as (
    select timestamp '2026-08-18 00:00:00' as window_end
),

windows as (
    select
        7 as window_days,
        window_end - interval '7' day as window_start,
        window_end
    from params

    union all

    select
        30 as window_days,
        window_end - interval '30' day as window_start,
        window_end
    from params
),

fee_receivers(address) as (
    values
        ('8r2hZoDfk5hDWJ1sDujAi2Qr45ZyZw5EQxAXiMZWLKh2'),
        ('Cj297UauzMX64FU9dKJZRUBWszJ7tEWpVheasq4CfATV'),
        ('HKMh8nV3ysSofRi23LsfVGLGQKB415QAEfZT96kCcVj4'),
        ('7tQiiBdKoScWQkB1RmVuML7DBGnR31cuKPEtMM7Vy5SA'),
        ('4BBNEVRgrxVKv9f7pMNE788XM1tt379X9vNjpDH2KCL7'),
        ('47hEzz83VFR23rLTEeVm9A7eFzjJwjvdupPPmX3cePqF'),
        ('EMbqD9Y9jLXEa3RbCR8AsEW1kVa3EiJgDLVgvKh4qNFP'),
        ('Lk693UiTzQC4vobasRS1QGcYA9D6RGYLjHp1bWreQtM')
),

fee_activity as (
    select
        fee_payments.block_time,
        fee_payments.tx_id,
        fee_payments.fee_receiver,
        fee_payments.fee_receiver_signed
    from dune.<ci_schema>.banana_gun_solana_fee_payments_raw as fee_payments
    cross join params
    where fee_payments.block_month >= cast(
            date_trunc('month', window_end - interval '30' day) as date
        )
        and fee_payments.block_time >= window_end - interval '30' day
        and fee_payments.block_time < window_end
),

transactions_30d as (
    select
        transactions.id,
        transactions.block_time,
        transactions.signer
    from solana.transactions as transactions
    cross join params
    where transactions.block_date >= cast(
            window_end - interval '30' day as date
        )
        and transactions.block_date < cast(window_end as date)
        and transactions.block_time >= window_end - interval '30' day
        and transactions.block_time < window_end
),

comparison as (
    select
        fee_activity.*,
        transactions_30d.id as transaction_id,
        transactions_30d.signer as primary_signer,
        transactions_30d.signer in (
            select address from fee_receivers
        ) as primary_signer_is_fee_receiver
    from fee_activity
    left join transactions_30d
        on transactions_30d.id = fee_activity.tx_id
        and transactions_30d.block_time = fee_activity.block_time
)

select
    windows.window_days,
    count(comparison.tx_id) as fee_rows,
    count_if(
        comparison.transaction_id is not null
        and comparison.primary_signer_is_fee_receiver
    ) as primary_signer_fee_wallet_rows,
    count_if(comparison.fee_receiver_signed) as fee_receiver_signed_rows,
    count_if(
        comparison.transaction_id is not null
        and comparison.primary_signer_is_fee_receiver
            = comparison.fee_receiver_signed
    ) as matching_rows,
    count_if(
        comparison.transaction_id is not null
        and comparison.primary_signer_is_fee_receiver
            <> comparison.fee_receiver_signed
    ) as mismatching_rows,
    count_if(
        comparison.transaction_id is not null
        and comparison.fee_receiver_signed
        and not comparison.primary_signer_is_fee_receiver
    ) as secondary_signer_only_rows,
    count_if(comparison.transaction_id is null) as missing_transaction_rows
from windows
left join comparison
    on comparison.block_time >= windows.window_start
    and comparison.block_time < windows.window_end
group by windows.window_days
order by windows.window_days;

-- If mismatching_rows or missing_transaction_rows is non-zero, rerun the CTEs
-- with this final SELECT to inspect up to 100 examples:
--
-- select *
-- from comparison
-- where transaction_id is null
--     or primary_signer_is_fee_receiver <> fee_receiver_signed
-- order by block_time desc
-- limit 100;
