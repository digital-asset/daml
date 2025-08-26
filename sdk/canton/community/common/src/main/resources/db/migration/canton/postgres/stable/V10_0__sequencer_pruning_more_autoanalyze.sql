-- By default analyze is triggered when a table has been vacuumed or when considerable part of the table changed.
-- For very large tables (auto-)vacuuming is too slow, leading to statistics not being updated often enough.
-- This leads to suboptimal query plans (falling back to Seq Scans), which can be avoided by running analyze more often.
-- We use 1'000'000 rows as a threshold, with the reasoning: not too often, but enough to keep the query planner happy.
alter table sequencer_events
    set (
        autovacuum_analyze_scale_factor = 0.0,
        autovacuum_analyze_threshold = 1000000
        );

alter table sequencer_payloads
    set (
        autovacuum_analyze_scale_factor = 0.0,
        autovacuum_analyze_threshold = 1000000
        );

alter table seq_block_height
    set (
        autovacuum_analyze_scale_factor = 0.0,
        autovacuum_analyze_threshold = 1000000
        );

alter table seq_traffic_control_consumed_journal
    set (
        autovacuum_analyze_scale_factor = 0.0,
        autovacuum_analyze_threshold = 1000000
        );

alter table seq_in_flight_aggregated_sender
    set (
        autovacuum_analyze_scale_factor = 0.0,
        autovacuum_analyze_threshold = 1000000
        );

alter table seq_in_flight_aggregation
    set (
        autovacuum_analyze_scale_factor = 0.0,
        autovacuum_analyze_threshold = 1000000
        );
