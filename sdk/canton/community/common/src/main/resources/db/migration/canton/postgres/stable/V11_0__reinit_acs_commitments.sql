-- Stores commitment reinitialization status (start, end)
create table par_commitment_reinitialization (
    synchronizer_idx integer not null,
    -- UTC timestamp in microseconds relative to EPOCH indicating whether a repair is ongoing for a timestamp
    ts_reinit_ongoing bigint,
    -- UTC timestamp in microseconds relative to EPOCH indicating the timestamp of the last completed reinitialization
    ts_reinit_completed bigint,
    primary key (synchronizer_idx)
);
