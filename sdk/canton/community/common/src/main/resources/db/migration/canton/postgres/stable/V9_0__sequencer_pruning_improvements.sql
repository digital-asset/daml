
create index sequencer_event_recipients_ts_idx on sequencer_event_recipients(ts);

create index seq_in_flight_aggregation_max_sequencing_time_idx on seq_in_flight_aggregation(max_sequencing_time);

-- Note: *_threshold is 10x of the other tables, since this table has many more rows.
alter table sequencer_event_recipients
    set (
        autovacuum_vacuum_scale_factor = 0.0,
        autovacuum_vacuum_threshold = 100000,
        autovacuum_vacuum_cost_limit = 2000,
        autovacuum_vacuum_cost_delay = 5,
        autovacuum_vacuum_insert_scale_factor = 0.0,
        autovacuum_vacuum_insert_threshold = 1000000,
        -- Note: auto vacuuming this table is too slow, so we need to run analyze more often than vacuuming completes:
        autovacuum_analyze_scale_factor = 0.0,
        autovacuum_analyze_threshold = 10000000
    );
