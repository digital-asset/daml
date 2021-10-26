DROP INDEX participant_events_create_tree_event_witnesses_idx;
DROP INDEX participant_events_create_flat_event_witnesses_idx;
DROP INDEX participant_events_consuming_exercise_flat_event_witnesses_idx;
DROP INDEX participant_events_consuming_exercise_tree_event_witnesses_idx;
DROP INDEX participant_events_non_consuming_exercise_flat_event_witness_idx;
DROP INDEX participant_events_non_consuming_exercise_tree_event_witness_idx;
DROP INDEX participant_events_divulgence_tree_event_witnesses_idx;

CREATE SEARCH INDEX participant_events_create_tree_event_witnesses_idx ON participant_events_create (tree_event_witnesses)
    FOR JSON
    PARAMETERS('DATAGUIDE OFF SEARCH_ON TEXT SYNC (EVERY "FREQ=SECONDLY; INTERVAL=1")');

CREATE SEARCH INDEX participant_events_create_flat_event_witnesses_idx ON participant_events_create (flat_event_witnesses)
    FOR JSON
    PARAMETERS('DATAGUIDE OFF SEARCH_ON TEXT SYNC (EVERY "FREQ=SECONDLY; INTERVAL=1")');

CREATE SEARCH INDEX participant_events_consuming_exercise_flat_event_witnesses_idx ON participant_events_consuming_exercise (flat_event_witnesses)
    FOR JSON
    PARAMETERS('DATAGUIDE OFF SEARCH_ON TEXT SYNC (EVERY "FREQ=SECONDLY; INTERVAL=1")');

CREATE SEARCH INDEX participant_events_consuming_exercise_tree_event_witnesses_idx ON participant_events_consuming_exercise (tree_event_witnesses)
    FOR JSON
    PARAMETERS('DATAGUIDE OFF SEARCH_ON TEXT SYNC (EVERY "FREQ=SECONDLY; INTERVAL=1")');

CREATE SEARCH INDEX participant_events_non_consuming_exercise_flat_event_witness_idx ON participant_events_non_consuming_exercise (flat_event_witnesses)
    FOR JSON
    PARAMETERS('DATAGUIDE OFF SEARCH_ON TEXT SYNC (EVERY "FREQ=SECONDLY; INTERVAL=1")');

CREATE SEARCH INDEX participant_events_non_consuming_exercise_tree_event_witness_idx ON participant_events_non_consuming_exercise (tree_event_witnesses)
    FOR JSON
    PARAMETERS('DATAGUIDE OFF SEARCH_ON TEXT SYNC (EVERY "FREQ=SECONDLY; INTERVAL=1")');

CREATE SEARCH INDEX participant_events_divulgence_tree_event_witnesses_idx ON participant_events_divulgence (tree_event_witnesses)
    FOR JSON
    PARAMETERS('DATAGUIDE OFF SEARCH_ON TEXT SYNC (EVERY "FREQ=SECONDLY; INTERVAL=1")');