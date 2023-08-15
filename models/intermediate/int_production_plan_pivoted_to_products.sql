
-- monthly data table of the production plan is pivoted with product no as columns
with pivoted_products as (

    select
        monthly_date,
        max(case when (product_no='Product 1') then total_planned_quantity else null end) as product1,
        max(case when (product_no ='Product 2') then total_planned_quantity else null end) as product2
        
    from {{ ref('int_production_plan_week_to_month_aggregation') }}
    
    group by monthly_date
    order by monthly_date
)

select * from pivoted_products