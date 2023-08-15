
-- daily data of manufactured units is aggregated to monthly
with month_aggregation as (

    select 
        extract(month from date) as date_month,
        extract(year from date) as date_year,
        sum(product1) as product1,
        sum(product2) as product2
    
    from {{ ref('postgresql__manufactured_units') }}
    
    group by extract(month from date), extract(year from date)
    order by extract(month from date)
),

-- date column is cleaned to display date of first day of the month
cleaned_monthly as (

    select
        to_date(concat(date_year, date_month), 'YYYYMM') as monthly_date,
        product1,
        product2
    
    from month_aggregation

)

select * from cleaned_monthly