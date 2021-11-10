-- This script extracts size limits for the append-only schema

-- Next steps:
--   * wrap in bash script extracting .csv files to current dir
--      - use percona scripts as a blueprint
--   * create DB before create_argument compression
--   * adapt script to that
--   * auto-detect DB version
-- 


select * from flyway_schema_history order by installed_rank;

select package_id, upload_id, source_description, known_since, ledger_offset, package_size as package_size_server_provided, octet_length(package) as package_size_stored_bytes, pg_column_size(package) as package_size_stored_compressed_bytes from packages order by ledger_offset;

select
    template_id, 
    exercise_choice,
    event_kind,
    max(pg_column_size(t.*)) as max_pg_row_size,
    create_argument_compression,
    max(octet_length(create_argument)) as max_create_argument_size,
    create_key_value_compression,
    max(octet_length(create_key_value)) as max_create_key_value_size,
    max(length(create_agreement_text)) as max_create_agreement_length,
    exercise_argument_compression,
    max(octet_length(exercise_argument)) as max_exercise_argument_size,
    exercise_result_compression,
    max(octet_length(exercise_result)) as max_exercise_result_size,
    max(array_length(submitters,1)) as max_num_submitters,
    max(length(array_to_string(submitters,''))) as max_text_length_submitters,
    max(array_length(create_signatories,1)) as max_num_create_signatories,
    max(length(array_to_string(create_signatories,''))) as max_text_length_create_signatories,
    max(array_length(create_observers,1)) as max_num_create_observers,
    max(length(array_to_string(create_observers,''))) as max_text_length_create_observers,
    max(array_length(exercise_actors,1)) as max_num_exercise_actors,
    max(length(array_to_string(exercise_actors,''))) as max_text_length_exercise_actors,
    max(array_length(flat_event_witnesses,1)) as max_num_flat_event_witnesses,
    max(length(array_to_string(flat_event_witnesses,''))) as max_text_length_flat_event_witnesses,
    max(array_length(tree_event_witnesses,1)) as max_num_tree_event_witnesses,
    max(length(array_to_string(tree_event_witnesses,''))) as max_text_length_tree_event_witnesses,
    max(array_length(exercise_child_event_ids,1)) as max_num_exercise_child_event_ids,
    max(length(array_to_string(exercise_child_event_ids,''))) as max_text_length_exercise_child_event_ids
  from
    participant_events as t
  group by 
    template_id,
    exercise_choice, 
    event_kind, 
    create_argument_compression,
    create_key_value_compression,
    exercise_argument_compression,
    exercise_result_compression
  order by 
    max(pg_column_size(t.*)) desc;


#!/bin/bash

set -e

PREFIX=' [%p-%s-%c-%l-%h-%u-%d-%a-%m] '

# for u in $(ls *gz)
for u in $(ls *log)
do
    echo "processing $u ..."
#   LOG=$(basename -s .log.gz $u)
    LOG=$(basename -s .log $u)

    pgbadger -f stderr -p "$PREFIX" --disable-connection --disable-session \
                                    --disable-lock --disable-query \
                                    --title "$LOG" -o $LOG.html $u
done