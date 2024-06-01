sqoop import \
    --connect jdbc:postgresql://172.17.0.3:5432/northwind \
    --username postgres \
    --query "select o.order_id , o.shipped_date , c.company_name , phone
            from orders o
            left join customers c 
            on o.customer_id = c.customer_id 
            where \$CONDITIONS" \
    --password-file file:///home/hadoop/scripts/pass.password \
    --target-dir /sqoop/ingest/envios \
    --as-parquetfile \
    --split-by o.order_id \
    --delete-target-dir