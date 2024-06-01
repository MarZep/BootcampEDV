sqoop import \
    --connect jdbc:postgresql://172.17.0.3:5432/northwind \
    --username postgres \
    --query "select od.order_id , od.unit_price , od.quantity , od.discount 
            from order_details od
            where \$CONDITIONS" \
    --password-file file:///home/hadoop/scripts/pass.password \
    --target-dir /sqoop/ingest/orders_details \
    --as-parquetfile \
    --split-by od.order_id \
    --delete-target-dir