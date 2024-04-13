-- Ejercicios SQL Wf

--#1
select c.category_name , p.product_name , p.unit_price ,
avg(p.unit_price )over (partition By c.category_id) as avgpricebycategory
from products p left join categories c 
on p.category_id = c.category_id 
order by c.category_name asc

--#2
select avg(od.unit_price * od.quantity) over (partition by o.customer_id) as avgorderamount , *
from orders o left join order_details od 
on o.order_id = od.order_id 

--#3
select p.product_name , c.category_name , p.quantity_per_unit , od.unit_price , od.quantity,
avg(od.quantity) over (partition by c.category_name) as avgquantity
from products p 
left join categories c on p.category_id  = c.category_id
left join order_details od on p.product_id = od.product_id  
order by c.category_name, p.product_name asc

--#4
select customer_id  , order_date , 
min(order_date) over (partition by customer_id ) as earliestorderdate
from orders o

--#5
select product_id ,product_name , unit_price , category_id ,
max(unit_price) over (partition by category_id)
from products p 

--#6
select rank() over(order by sum(quantity) desc ) as ranking, p.product_name, sum(od.quantity) as totalquantity
from order_details od left join products p  
on od.product_id = p.product_id
group by p.product_name

--#7
select row_number() over(order by customer_id asc) as rownumber ,  *
from customers c 

--#8
select rank() over(order by birth_date desc) as ranking ,
concat(first_name,'', last_name) as employeename, birth_date 
from employees e 

--#9
select sum(od.unit_price * od.quantity) over(partition by customer_id), *
from orders o left join order_details od 
on o.order_id = od.order_id 

--#10
select c.category_name , p.product_name , od.unit_price , od.quantity,
sum(od.unit_price * od.quantity) over(partition by c.category_name) as totalsales
from products p 
left join order_details od  on od.product_id = p.product_id 
left join categories c on p.category_id = c.category_id
order by c.category_name , p.product_name 

--#11
select ship_country as country , order_id , shipped_date , freight ,
sum(freight) over(partition by ship_country) as totalshippingcosts
from orders o
order by ship_country, order_id asc

--#12
select c.customer_id , c.company_name , sum(od.unit_price * od.quantity) as totalsales,
RANK() over (order by sum(od.unit_price * od.quantity) desc)
from orders o 
left join customers c on c.customer_id = o.customer_id 
left join order_details od on o.order_id  = od.order_id 
group by c.customer_id 
	
--#13
select employee_id , first_name , last_name ,hire_date ,
rank() over ( order by hire_date asc)
from employees e 

--#14
select product_id , product_name , unit_price ,
rank() over(order by unit_price desc)
from products p 

--#15
select order_id , product_id , quantity ,
lag(quantity,1) over(order by order_id ) as prevquantity 
from order_details od

--#16
select order_id , order_date , customer_id ,
lag(order_date,1) over( partition by customer_id order by customer_id, order_date asc) as lastorderdate
from orders o 

--#17
select product_id ,product_name , unit_price ,
lag(unit_price,1) over(order by product_id) as lastunitprice,
(unit_price - lag(unit_price,1) over()) as pricedifference
from products p 

--#18
select product_name , unit_price ,
lead(unit_price,1) over() as nextprice
from products p 

--#19
select c.category_name , sum(od.unit_price * od.quantity) totalsales ,
lead(sum(od.unit_price * od.quantity),1) over(order by c.category_name asc) as nexttotalsales 
from products p
left join categories c on p.category_id = c.category_id 
left join order_details od  on p.product_id = od.product_id
group by c.category_name 
