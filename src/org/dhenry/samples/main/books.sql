-- Write an sql query to find books that have sold fewer than 10 copies in the 
-- last year, excluding books that have been available for less than 1 month.

select p.product_id, p.name, sum(o.quantity)
from product p, orders o
where p.available_from <= add_months(sysdate, -1)
and o.dispatch_date >= add_months(sysdate, -12)
and p.product_id = o.product_id
group by p.product_id, p.name
having sum(o.quantity) < 10
order by p.name;

-- Tested in Oracle Live SQL
-- PRODUCT_ID	NAME	SUM(O.QUANTITY)
-- 103	Learn Python in Ten Minutes	1

