{% snapshot customer_snapshot %}

    {{
        config(
            target_schema='MARTS',
            unique_key='customer_id',
            strategy='check',
            check_cols=['segment', 'preferred_channel', 'marketing_consent', 'lifetime_value', 'last_purchase_date']
        )
    }}

    select
        customer_id,
        first_name,
        last_name,
        email,
        segment,
        preferred_channel,
        marketing_consent,
        lifetime_value,
        last_purchase_date
    from {{ ref('stg_customers') }}

{% endsnapshot %}
