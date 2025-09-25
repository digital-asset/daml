-- Remove the redundant mediator_id column

drop index idx_mediator_deduplication_store_expire_after;

alter table mediator_deduplication_store
    drop column mediator_id;

create index idx_mediator_deduplication_store_expire_after on mediator_deduplication_store(expire_after);
