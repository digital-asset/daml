------------------------------------ ETQ Data migration -------------------------------

-- Removes all elements from a that are present in b, essentially computes a - b.
CREATE OR REPLACE FUNCTION etq_array_diff(a int[], b int[])
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
        etq_array_diff(tree_event_witnesses, flat_event_witnesses) AS ps
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
        etq_array_diff(tree_event_witnesses, flat_event_witnesses) AS ps
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
SELECT t, o, first_i, last_i FROM input2,parameters WHERE
          parameters.participant_pruned_up_to_inclusive is null
          or o > parameters.participant_pruned_up_to_inclusive;

DROP FUNCTION etq_array_diff;