drop table if exists public.temp_order_events_gs;

create table public.temp_order_events_gs
(
    id       serial
        primary key,
    order_id integer     not null,
    status   varchar(50) not null,
    ts       timestamp default CURRENT_TIMESTAMP
);