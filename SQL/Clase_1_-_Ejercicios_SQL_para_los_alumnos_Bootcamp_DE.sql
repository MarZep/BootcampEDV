--Ejercicios SQL

--#1
select distinct (category_name)
from categories c 

--#2
select distinct (region)
from customers c

--#3
select distinct (contact_title)
from customers c

--#4
select *
from customers c 
order by country asc 

--#5
select *
from orders o 
order by employee_id asc , order_date 

--#6
insert into customers (customer_id, company_name, contact_name, contact_title, country)
values	('BCD','Bootcamp','Martin Lopez','Sales_Engineer','Argentina')

--#7
insert into region (region_id, region_description)
values (6,'Latam')

--#8
select *
from customers c 
where c.region isnull 

--#9
select product_name , coalesce (unit_price, 10) as price 
from products p 

--#10
select c.company_name , c.contact_name, o.order_date  
from customers c inner join orders o
on c.customer_id = o.customer_id 

--#11
select od.order_id , p.product_name , od.discount 
from order_details od inner join products p 
on od.product_id = p.product_id 

--#12
select c.customer_id , c.company_name, o.order_id , o.order_date  
from orders o left join customers c 
on  c.customer_id = o.customer_id 

--#13
select et.employee_id , e.last_name , t.territory_id , t.territory_description 
from employee_territories et  
left join territories t on et.territory_id = t.territory_id
left join employees e  on et.employee_id  = e.employee_id 

--#14
select o.order_id , c.company_name 
from orders o left join customers c 
on o.customer_id = c.customer_id 

--#15
select o.order_id , c.company_name 
from customers c right join orders o  
on o.customer_id = c.customer_id 

--#16
select s.company_name , o.order_date 
from shippers s  right join orders o 
on s.shipper_id  = o.ship_via  
where o.order_date between '1996-01-01' AND '1996-12-31'

--#17
select e.first_name , e.last_name , et.territory_id 
from employee_territories et full outer join employees e 
on et.employee_id = e.employee_id 

--#18
select o.order_id , od.unit_price , od.quantity , (od.unit_price * od.quantity) as total
from orders o  full outer join order_details od  
on o.order_id = od.order_id  

--#19
select company_name as nombre 
from customers c 
union
select company_name
from suppliers s 

--#20
select first_name  as nombre 
from employees e  
union
select first_name 
from employees e 
where e.employee_id in 
	(select reports_to
	from employees e2 )

--#21
select p.product_name , p.product_id 
from products p 
where p.product_id in 
	(select distinct (product_id)
	from order_details od)

--#22
select company_name 
from customers c 
where c.customer_id in 
	(select customer_id
	 from orders o where ship_country = 'Argentina')

--#23
select product_name 
from products p 
where product_id not in 
	(select od.product_id 
	from orders o  
	left join customers c on o.customer_id = c.customer_id
	left join order_details od  on od.order_id = o.order_id 
	where c.country = 'France')

--#24
select order_id , sum(quantity) 
from order_details od  
group by order_id

--#25
select product_name , avg(units_in_stock) 
from products p 
group by product_name 

--#26
select product_name , sum(units_in_stock) 
from products p 
group by product_name 
having sum(units_in_stock) > 100 

--#27
select c.company_name  , avg(o.order_id) as averageorders
from orders o left join customers c 
on o.customer_id = c.customer_id 
group by c.company_name 
having avg(o.order_id) > 10

--#28
select p.product_name ,
case when p.discontinued = 1 then 'Discontinued'
else c.category_name 
end as product_category
from products p left join categories c 
on p.category_id  = c.category_id 

--#29
select first_name , last_name ,
case when title = 'Sales Manager' then 'Gerente de Ventas'
else title
end as job_title
from employees e 

