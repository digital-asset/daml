create or replace view debug.par_commitment_reinitialization as
  select
    debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
    debug.canton_timestamp(ts_reinit_ongoing) as ts_reinit_ongoing,
    debug.canton_timestamp(ts_reinit_completed) as ts_reinit_completed
  from par_commitment_reinitialization;
