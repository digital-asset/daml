alter table common_sequenced_events
    set (
        autovacuum_vacuum_scale_factor = 0.0,
        autovacuum_vacuum_threshold = 10000,
        autovacuum_vacuum_cost_limit = 2000,
        autovacuum_vacuum_cost_delay = 5,
        autovacuum_vacuum_insert_scale_factor = 0.0,
        autovacuum_vacuum_insert_threshold = 100000,
        -- By default analyze is triggered when a table has been vacuumed or when considerable part of the table changed.
        -- For very large tables (auto-)vacuuming is too slow, leading to statistics not being updated often enough.
        -- This leads to suboptimal query plans (falling back to Seq Scans), which can be avoided by running analyze more often.
        -- We use 1'000'000 rows as a threshold, with the reasoning: not too often, but enough to keep the query planner happy.
        autovacuum_analyze_scale_factor = 0.0,
        autovacuum_analyze_threshold = 999999
    );
