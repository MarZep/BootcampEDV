ruta='/home/nifi/ingest'

rm -f "${ruta}"/titanic.csv

wget -P "${ruta}" https://dataengineerpublic.blob.core.windows.net/data-engineer/titanic.csv