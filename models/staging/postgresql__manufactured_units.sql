
-- staging model for the data of manufactured units
with manufactured_units as (
    
    select
        date,
        product1,
        product2
    
    from postgres.public.man_units
)

select * from manufactured_units