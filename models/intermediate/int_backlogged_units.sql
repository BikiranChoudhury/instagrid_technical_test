
-- calculate the cumulative backlogged units each month

-- calculate cumulative production plan per month
with cumulative_prod_plan as (

    select 
        monthly_date,
        SUM(product1) over(order by monthly_date rows between unbounded preceding and current row) as product1_cum,
        SUM(product2) over(order by monthly_date rows between unbounded preceding and current row) as product2_cum

    from {{ ref('int_production_plan_pivoted_to_products') }} 

    order by monthly_date
),

-- calculate cumulative manufactured units per month
cumulative_man_units as (

    select
        monthly_date,
        SUM(product1) over(order by monthly_date rows between unbounded preceding and current row) as product1_cum,
        SUM(product2) over(order by monthly_date rows between unbounded preceding and current row) as product2_cum

    from {{ ref('int_manufactured_units_day_to_month_aggregation') }} 

    order by monthly_date
),

-- find the difference between planned and manufactured each month
cumulative_backlog_each_month as (

    select
        cumulative_prod_plan.monthly_date,
        cumulative_prod_plan.product1_cum - cumulative_man_units.product1_cum as product1,
        cumulative_prod_plan.product2_cum - cumulative_man_units.product2_cum as product2
    
    from cumulative_prod_plan
    left join cumulative_man_units using (monthly_date)
)

select * from cumulative_backlog_each_month