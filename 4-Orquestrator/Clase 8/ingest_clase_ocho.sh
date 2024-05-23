ruta='/home/hadoop/landing/'
ruta_hdfs='/home/hadoop/hadoop/bin/'

rm -f "${ruta}"*.*

wget -P "${ruta}" https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/results.csv

wget -P "${ruta}" https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/drivers.csv

wget -P "${ruta}" https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/constructors.csv

wget -P "${ruta}" https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/races.csv

"${ruta_hdfs}hdfs" dfs -rm /ingest/*.*

"${ruta_hdfs}hdfs" dfs -put "${ruta}"*.* /ingest