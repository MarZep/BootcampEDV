ruta='/home/hadoop/landing/'
ruta_hdfs='/home/hadoop/hadoop/bin/'

rm -f "${ruta}"*.*

wget -P "${ruta}" https://dataengineerpublic.blob.core.windows.net/data-engineer/2021-informe-ministerio.csv

wget -P "${ruta}" https://dataengineerpublic.blob.core.windows.net/data-engineer/202206-informe-ministerio.csv

wget -P "${ruta}" https://dataengineerpublic.blob.core.windows.net/data-engineer/aeropuertos_detalle.csv

"${ruta_hdfs}hdfs" dfs -rm /ingest/*.*

"${ruta_hdfs}hdfs" dfs -put "${ruta}"*.* /ingest

