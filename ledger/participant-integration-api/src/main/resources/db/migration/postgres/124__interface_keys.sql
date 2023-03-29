-- Stores all create events for contracts of an interface key.
CREATE TABLE participant_events_create_interface_key(
    event_sequential_id bigint NOT NULL,      -- event identification: same ordering as event_offset
    interface_id INTEGER NOT NULL,
    create_key_value BYTEA NOT NULL,
    create_key_hash TEXT NOT NULL, -- key hash includes a hash of the interface_id
    UNIQUE (interface_id, event_sequential_id),
);

CREATE INDEX participant_events_create_interface_key_key_hash_idx ON participant_events_create_interface_key USING btree (create_key_hash, event_sequential_id);
