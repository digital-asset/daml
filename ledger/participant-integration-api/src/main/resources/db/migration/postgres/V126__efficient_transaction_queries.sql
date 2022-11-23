
-- Completions

DROP INDEX participant_command_completion_offset_application_idx;
CREATE INDEX participant_command_completions_application_id_offset_idx ON participant_command_completions USING btree (application_id, completion_offset);

-- Flat transactions

ALTER TABLE participant_events_create_filter RENAME
         TO pe_create_id_filter_stakeholder;
ALTER INDEX idx_participant_events_create_filter_party_template_seq_id_idx RENAME
         TO pe_create_id_filter_stakeholder_pts_idx;
ALTER INDEX idx_participant_events_create_filter_party_seq_id_idx RENAME
         TO pe_create_id_filter_stakeholder_pt_idx;
ALTER INDEX idx_participant_events_create_seq_id_idx RENAME
         TO pe_create_id_filter_stakeholder_s_idx;

CREATE TABLE pe_consuming_id_filter_stakeholder (
   event_sequential_id BIGINT NOT NULL,
   template_id INTEGER NOT NULL,
   party_id INTEGER NOT NULL
);
CREATE INDEX pe_consuming_id_filter_stakeholder_pts_idx ON pe_consuming_id_filter_stakeholder(party_id, template_id, event_sequential_id);
CREATE INDEX pe_consuming_id_filter_stakeholder_ps_idx  ON pe_consuming_id_filter_stakeholder(party_id, event_sequential_id);
CREATE INDEX pe_consuming_id_filter_stakeholder_s_idx   ON pe_consuming_id_filter_stakeholder(event_sequential_id);


--- Tree transactions

CREATE TABLE pe_create_id_filter_non_stakeholder_informee (
   event_sequential_id BIGINT NOT NULL,
   party_id INTEGER NOT NULL
);
CREATE INDEX pe_create_id_filter_non_stakeholder_informee_ps_idx ON pe_create_id_filter_non_stakeholder_informee(party_id, event_sequential_id);
CREATE INDEX pe_create_id_filter_non_stakeholder_informee_s_idx ON pe_create_id_filter_non_stakeholder_informee(event_sequential_id);


CREATE TABLE pe_consuming_id_filter_non_stakeholder_informee (
   event_sequential_id BIGINT NOT NULL,
   party_id INTEGER NOT NULL
);
CREATE INDEX pe_consuming_id_filter_non_stakeholder_informee_ps_idx ON pe_consuming_id_filter_non_stakeholder_informee(party_id, event_sequential_id);
CREATE INDEX pe_consuming_id_filter_non_stakeholder_informee_s_idx ON pe_consuming_id_filter_non_stakeholder_informee(event_sequential_id);


CREATE TABLE pe_non_consuming_id_filter_informee (
   event_sequential_id BIGINT NOT NULL,
   party_id INTEGER NOT NULL
);
CREATE INDEX pe_non_consuming_id_filter_informee_ps_idx ON pe_non_consuming_id_filter_informee(party_id, event_sequential_id);
CREATE INDEX pe_non_consuming_id_filter_informee_s_idx ON pe_non_consuming_id_filter_informee(event_sequential_id);


-- Point-wise lookup

CREATE TABLE participant_transaction_meta(
    transaction_id TEXT NOT NULL,
    event_offset TEXT NOT NULL,
    event_sequential_id_first BIGINT NOT NULL,
    event_sequential_id_last BIGINT NOT NULL
);
CREATE INDEX participant_transaction_meta_tid_idx ON participant_transaction_meta(transaction_id);
CREATE INDEX participant_transaction_meta_event_offset_idx ON participant_transaction_meta(event_offset);

DROP INDEX participant_events_create_transaction_id_idx;
DROP INDEX participant_events_consuming_exercise_transaction_id_idx;
DROP INDEX participant_events_non_consuming_exercise_transaction_id_idx;

--------------------------------------------------------
--------- Data migration -------------------------------
--------------------------------------------------------

-- Removes all elements from a that are present in b, essentially computes a - b.
CREATE OR REPLACE FUNCTION array_diff(a int[], b int[])
RETURNS int[]
AS
$$
      SELECT coalesce(array_agg(el), '{}')
      FROM unnest(a) as el
      WHERE el <> all(b)
$$
LANGUAGE SQL;

-- Populate pe_create_id_filter_non_stakeholder_informee
WITH
input1 AS
(
    SELECT
        event_sequential_id AS i,
        array_diff(tree_event_witnesses, flat_event_witnesses) AS ps
    FROM participant_events_create
)
INSERT INTO pe_create_id_filter_non_stakeholder_informee(event_sequential_id, party_id)
SELECT i, unnest(ps) FROM input1;

-- Populate pe_consuming_id_filter_stakeholder
WITH
input1 AS
(
    SELECT
        event_sequential_id AS i,
        template_id AS t,
        flat_event_witnesses AS ps
    FROM participant_events_consuming_exercise
)
INSERT INTO pe_consuming_id_filter_stakeholder(event_sequential_id, template_id, party_id)
SELECT i, t, unnest(ps) FROM input1;

-- Populate pe_consuming_id_filter_non_stakeholder_informee
WITH
input1 AS
(
    SELECT
        event_sequential_id AS i,
        array_diff(tree_event_witnesses, flat_event_witnesses) AS ps
    FROM participant_events_consuming_exercise
)
INSERT INTO pe_consuming_id_filter_non_stakeholder_informee(event_sequential_id, party_id)
SELECT i, unnest(ps) FROM input1;

-- Populate pe_non_consuming_exercise_filter_nonstakeholder_informees
WITH
input1 AS
(
    SELECT
        event_sequential_id AS i,
        tree_event_witnesses AS ps
    FROM participant_events_non_consuming_exercise
)
INSERT INTO pe_non_consuming_id_filter_informee(event_sequential_id, party_id)
SELECT i, unnest(ps) FROM input1;

-- Populate participant_transaction_meta
WITH
input1 AS (
        SELECT
            transaction_id AS t,
            event_offset AS o,
            event_sequential_id AS i
        FROM participant_events_create
    UNION ALL
        SELECT
            transaction_id AS t,
            event_offset AS o,
            event_sequential_id AS i
        FROM participant_events_consuming_exercise
    UNION ALL
        SELECT
            transaction_id AS t,
            event_offset AS o,
            event_sequential_id AS i
        FROM participant_events_non_consuming_exercise
    UNION ALL
        -- NOTE: Divulgence offsets with no corresponding create events will not
        --       have an entry in transaction_meta table
        SELECT
            c.transaction_id AS t,
            c.event_offset AS o,
            d.event_sequential_id AS i
        FROM participant_events_divulgence d
        JOIN participant_events_create c ON d.contract_id = c.contract_id
),
input2 AS (
    SELECT
        t,
        o,
        min(i) as first_i,
        max(i) as last_i
    FROM input1
    GROUP BY t, o
)
INSERT INTO participant_transaction_meta(transaction_id, event_offset, event_sequential_id_first, event_sequential_id_last)
SELECT t, o, first_i, last_i FROM input2;
