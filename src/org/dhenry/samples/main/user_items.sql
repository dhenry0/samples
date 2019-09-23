#1
select u.first_name, u.last_name, count(*) "purchased"
from users u, items i, user_items ui
where u.id = ui.user_id and ui.item_id = i.id
group by u.first_name, u.last_name
having count(*) >= 1
order by u.last_name, u.first_name;

#2
select u.id, u.first_name, u.last_name
from users u where not exists
	(select 1 from user_items where user_id = u.id);

#3
select i.description, count(*) "num_purchased", sum(cost_pennies) "revenue"
from user_items ui, items i
where ui.item_id = i.id
group by i.description
order by num_purchased desc;

#4
select u.first_name, u.last_name, max(i.description) "description", max(ui.purchased_at) "purchased_at"
from users u, items i, user_items ui
where u.id = ui.user_id and i.id = ui.item_id
group by u.first_name, u.last_name
having count(*) >= 1
order by 4 desc;

