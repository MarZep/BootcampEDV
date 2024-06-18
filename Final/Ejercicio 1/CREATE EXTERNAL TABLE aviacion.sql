CREATE EXTERNAL TABLE aviacion.aeropuerto_tabla(fecha date, horaUTC string, clase_de_vuelo string, clasificacion_de_vuelo string, tipo_de_movimiento string, aeropuerto string, origen_destino string, aerolinea_nombre string, aeronave string, pasajeros int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tables/external/aviacion/aeropuerto_tabla';

CREATE EXTERNAL TABLE aviacion.aeropuerto_detalle_tabla(aeropuerto string, oac string, iata string, tipo string, denominacion string, coordenadas string, latitud string, longitud string, elev float, uom_elev string, ref string, distancia_ref float, direccion_ref string, condicion string, control string, region string, uso string, trafico string, sna string, concesionado string, provincia string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tables/external/aviacion/aeropuerto_detalle_tabla';

