select count(*) as cantidad_vuelos
from aeropuerto_tabla at
where fecha BETWEEN '2021-12-01' and '2022-01-31'

select SUM(pasajeros) as cantidad_pasajeros
from aeropuerto_tabla at
where aerolinea_nombre = 'AEROLINEAS ARGENTINAS SA' and fecha BETWEEN '2021-01-01' and '2022-06-30'

select at.fecha, at.horautc , at.aeropuerto, adt_origen.ref as aeropuerto_ciudad, at.origen_destino, adt_salida.ref as origen_destino_ciudad, SUM(at.pasajeros) as cantidad_pasajeros
from aeropuerto_tabla at
left join aeropuerto_detalle_tabla adt_origen on at.aeropuerto = adt_origen.aeropuerto
left join aeropuerto_detalle_tabla adt_salida on at.origen_destino  = adt_salida.aeropuerto 
where at.aerolinea_nombre = 'AEROLINEAS ARGENTINAS SA' and at.fecha BETWEEN '2022-01-01' and '2022-06-30'
GROUP by at.fecha, at.horautc , at.aeropuerto, at.origen_destino,adt_salida.ref,adt_origen.ref
order by at.fecha DESC 

select aerolinea_nombre, SUM(pasajeros) as cantidad_pasajeros
from aeropuerto_tabla at
where aerolinea_nombre NOT LIKE '0' and fecha BETWEEN '2021-01-01' and '2022-06-30'
group by aerolinea_nombre
order by cantidad_pasajeros DESC
limit 10

select at.aeronave, COUNT(at.aeronave) as cantidad
from aeropuerto_tabla at
left join aeropuerto_detalle_tabla adt on at.aeropuerto = adt.aeropuerto 
where at.aeronave NOT LIKE '0'
and at.tipo_de_movimiento = 'Despegue'
and (adt.provincia  = 'BUENOS AIRES' or adt.provincia  = 'CIUDAD AUTÓNOMA DE BUENOS AIRES')
and at.fecha BETWEEN '2021-01-01' and '2022-06-30'
group by at.aeronave 
order by cantidad DESC
limit 10


