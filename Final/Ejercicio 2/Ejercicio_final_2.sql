select count(*) as cantidad_alquileres
from car_rental_analytics cra 
where rating >= 4 and (fueltype ='hybrid' or fueltype = 'electric')

select year, count(*) as cantidad_alquileres
from car_rental_analytics cra 
where year >= 2010 and year <= 2015
GROUP by year  

select fueltype, ROUND(avg(reviewcount),2) as cantidad_reviews
from car_rental_analytics cra 
GROUP by fueltype
ORDER by cantidad_reviews desc

