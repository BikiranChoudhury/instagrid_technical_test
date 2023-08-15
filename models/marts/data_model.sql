
-- build the final data model with the intermediate files of
-- production plan, manufactured units and backlog

with final_prod_plan as (

    select * from {{ ref('int_production_plan_week_to_month_aggregation') }}

),

-- aggregated monthly manufactured data is normalised
final_man_units as (

    select
        monthly_date, t.*

    from {{ ref('int_manufactured_units_day_to_month_aggregation') }} m

    cross join lateral (
        values
            (m.product1, 'Product 1'),
            (m.product2, 'Product 2')
        ) as t(manufactured_units, product_no)
    
    order by monthly_date, product_no
),

-- cummulative backlog data is normalised
final_backlog as (

    select
        monthly_date, t.*

    from {{ ref('int_backlogged_units') }} b

    cross join lateral (
        values
            (b.product1, 'Product 1'),
            (b.product2, 'Product 2')
        ) as t(backlog, product_no)
    
    order by monthly_date, product_no
),

-- finally join all the three tables
final as (

    select
        monthly_date,
        product_no,
        total_planned_quantity,
        final_man_units.manufactured_units,
        final_backlog.backlog

    from final_prod_plan

    left join final_man_units using (monthly_date, product_no)
    left join final_backlog using (monthly_date, product_no)

    order by monthly_date, product_no

)

select * from final