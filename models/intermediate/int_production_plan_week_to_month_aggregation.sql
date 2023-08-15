
-- Weekly data of production plan is aggregated to monthly 

-- first the week no is extracted as an integer from the week column
with extract_week_no as (

    select
        trim(leading 'W' from week)::numeric as week_no,
        product_no,
        planned_quantity,
        year
    
    from {{ ref('postgresql__production_plan') }}
),

-- based on the week number and year, the corresponding month is derived
-- first day of the week is considered to determine the month
derive_month as (

    select
        extract(month from to_date(concat(year,week_no),'IYYYIW')) as month_no,
        product_no,
        planned_quantity,
        year
    
    from extract_week_no
),

-- date column is cleaned to display date of first day of the month
cleaned_monthly as (

    select
        to_date(concat(year, month_no), 'YYYYMM') as monthly_date,
        product_no,
        planned_quantity
    
    from derive_month
),

-- the production plan is summed to aggregate on monthly level
monthly_aggregation as (

    select
        monthly_date,
        product_no,
        sum(planned_quantity) as total_planned_quantity

    from cleaned_monthly
    
    group by monthly_date, product_no
    order by monthly_date, product_no
)

select * from monthly_aggregation