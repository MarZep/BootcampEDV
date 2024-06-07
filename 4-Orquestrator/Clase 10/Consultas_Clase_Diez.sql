--a) Cuántos hombres y cuántas mujeres sobrevivieron
select sex, COUNT(i.sex) as total_sobrevivientes  
from titanic.information i 
where i.survived = 1
GROUP BY i.sex 

--b) Cuántas personas sobrevivieron según cada clase (Pclass)
select pclass, COUNT(i.pclass) as total_sobrevivientes  
from titanic.information i 
where i.survived = 1
GROUP BY i.pclass 

--c) Cuál fue la persona de mayor edad que sobrevivió
select *  
from titanic.information i 
where i.survived = 1
order by i.age desc limit 1

--d) Cuál fue la persona más joven que sobrevivió
select *  
from titanic.information i 
where i.survived = 1 and i.age IS NOT NULL
order by i.age asc limit 1
