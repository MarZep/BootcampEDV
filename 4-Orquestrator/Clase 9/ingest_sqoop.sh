sqoop import \
    -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
    --connect jdbc:postgresql://172.17.0.3:5432/northwind \
    --username postgres \
    --query "select c.customer_id, c.company_name, SUM(od.product_id) as
            productos_vendidos from customers c 
            left join orders o on c.customer_id = o.customer_id
            left join order_details od on o.order_id = od.order_id 
            group by c.customer_id, c.company_name 
            having \$CONDITIONS
            order by productos_vendidos desc" \
    --password-file file:///home/hadoop/scripts/pass.password \
    --target-dir /sqoop/ingest/clientes \
    --as-parquetfile \
    --split-by c.customer_id \
    --delete-target-dir
