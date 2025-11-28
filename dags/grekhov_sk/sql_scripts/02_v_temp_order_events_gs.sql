drop view if exists public.v_temp_order_events_gs;

create view public.v_temp_order_events_gs as
SELECT
    id,
    order_id,
    status,
    ts
FROM public.temp_order_events_gs;

select 
    id,
    order_id,
    status,
    ts
from public.v_temp_order_events_gs