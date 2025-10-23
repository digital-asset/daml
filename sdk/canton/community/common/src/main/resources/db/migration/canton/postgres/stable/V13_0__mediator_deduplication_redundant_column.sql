-- Remove the redundant mediator_id column

drop index idx_mediator_deduplication_store_expire_after;

drop view debug.mediator_deduplication_store;

alter table mediator_deduplication_store
    drop column mediator_id;

create or replace view debug.mediator_deduplication_store as
select
    uuid,
    debug.canton_timestamp(request_time) as request_time,
    debug.canton_timestamp(expire_after) as expire_after
from mediator_deduplication_store;

create index idx_mediator_deduplication_store_expire_after on mediator_deduplication_store(expire_after);
