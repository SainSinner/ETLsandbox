with rnk_status_order as (SELECT order_id,
                                 status,
                                 ts_data,
                                 rank() over (partition by order_id order by ts_data) as rnk
                          FROM default.orders_stream
                          where status <> '')
select
    order_id,
    status,
    toDateTime(ts_data) as ts_data,
    1 as check
from rnk_status_order
where rnk = 1
order by ts_data