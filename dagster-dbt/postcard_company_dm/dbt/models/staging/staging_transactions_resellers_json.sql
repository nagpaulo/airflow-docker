{{
    config(
        materialized='incremental',
        schema='staging'
    )
}}

WITH 

  {% if is_incremental() %}

latest_transaction AS (
    
select MAX(loaded_timestamp) AS max_transaction FROM {{ this }}

),

  {% endif %}

trans_json AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(
      [ "'reseller-id'", 'transactionId']
    ) }} AS customer_key,
    "reseller-id" AS reseller_id,
    transactionId AS transaction_id,
    productName AS product_name,
    totalAmount AS total_amount,
    qty AS no_purchased_postcards,
    "date" AS bought_date,
    salesChannel AS sales_channel,
    seriesCity AS office_location,
    loaded_timestamp
  FROM
    {{ ref(
      'raw_resellers_json'
    ) }}


  {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where loaded_timestamp > (SELECT max_transaction FROM latest_transaction LIMIT 1)

  {% endif %}



)


SELECT
  t.customer_key,
  transaction_id,
  e.product_key,
  C.channel_key,
  t.reseller_id,
  bought_date AS bought_date_key,
  total_amount::NUMERIC AS total_amount,
  no_purchased_postcards,
  e.product_price::NUMERIC AS product_price,
  e.geography_key,
  s.commission_pct * total_amount::NUMERIC AS commissionpaid,
  s.commission_pct AS commissionpct,
  loaded_timestamp
FROM
  trans_json t
  JOIN {{ ref('dim_product') }} e
    ON t.product_name = e.product_name
  JOIN {{ ref('dim_channel') }} C
    ON t.sales_channel = C.channel_name
  JOIN {{ ref('dim_customer') }} cu
    ON t.customer_key = cu.customer_key
  JOIN {{ ref('dim_salesagent') }} s
    ON t.reseller_id = s.original_reseller_id