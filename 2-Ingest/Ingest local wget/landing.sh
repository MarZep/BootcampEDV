#wget -P /home/hadoop/landing https://github.com/fpineyro/homework-0/blob/master/starwars.csv

wget -P /home/hadoop/landing https://raw.githubusercontent.com/fpineyro/homework-0/master/starwars.csv

hdfs dfs -put /home/hadoop/landing/starwars.csv /ingest

if hdfs dfs -test -e /ingest/starwars.csv; then
    echo "borrando archivo Starwars.csv"
	rm -r /home/hadoop/landing/starwars.csv
else
    echo "El archivo no existe."
fi