CREATE EXTERNAL TABLE car_rental_db.car_rental_analytics(fuelType string, rating integer, renterTripsTaken integer, reviewCount integer, city string, state_name string, owner_id integer, rate_daily integer, make string, model string, year integer)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tables/external/car_rental_db/car_rental_analytics';

