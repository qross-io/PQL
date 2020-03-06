DEBUG ON;

OPEN mysql.rds;
select eff_date, end_date from zichan360bi_analysis.client_product_dim_type ORDER BY id LIMIT 10;