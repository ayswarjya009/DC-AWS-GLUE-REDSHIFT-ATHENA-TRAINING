select order_id, customer_name, item_name, item_price, order_date
from hks_sample_db.orders
where extract(day from order_date)=23 and
extract(month from order_date)=8
and extract(year from order_date)=2024
