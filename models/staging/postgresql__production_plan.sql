
-- staging model for the production plan data
with production_plan as (
    
    select
        week,
        pn as product_no,
        plan as planned_quantity,
        year
    
    from postgres.public.prod_plan
)

select * from production_plan