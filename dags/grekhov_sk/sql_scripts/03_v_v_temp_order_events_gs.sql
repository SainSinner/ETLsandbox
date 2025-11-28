drop view if exists public.v_v_temp_order_events_gs;

create view public.v_v_temp_order_events_gs as
SELECT
    id,
    order_id,
    status,
    ts
FROM public.v_temp_order_events_gs;

select 
    id,
    order_id,
    status,
    ts
from public.v_v_temp_order_events_gs