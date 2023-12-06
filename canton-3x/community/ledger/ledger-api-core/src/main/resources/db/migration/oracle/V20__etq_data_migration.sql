------------------------------------ ETQ Data migration -------------------------------

-- Removes all elements from a that are present in b, essentially computes a - b.
CREATE OR REPLACE FUNCTION etq_array_diff(
    clobA IN CLOB,
    clobB IN CLOB
)
RETURN CLOB
IS
     aDiffB CLOB;
BEGIN
    SELECT coalesce(JSON_ARRAYAGG(elemA), '[]') foo
    INTO aDiffB
    FROM
        (
                SELECT elemA FROM json_table(clobA, '$[*]' columns (elemA NUMBER PATH '$'))
        ) arrayA
    LEFT JOIN
        (
                SELECT elemB FROM json_table(clobB, '$[*]' columns (elemB NUMBER PATH '$'))
        ) arrayB
    ON elemA = elemB
    WHERE elemB IS NULL;
    RETURN aDiffB;
END;
/

-- Populate pe_create_id_filter_non_stakeholder_informee
INSERT INTO pe_create_id_filter_non_stakeholder_informee(event_sequential_id, party_id)
WITH
input1 AS
(
    SELECT
        event_sequential_id AS i,
        etq_array_diff(tree_event_witnesses, flat_event_witnesses) AS ps
    FROM participant_events_create
)
SELECT i, p
FROM input1, json_table(ps, '$[*]' columns (p NUMBER PATH '$'));

-- Populate pe_consuming_id_filter_stakeholder
INSERT INTO pe_consuming_id_filter_stakeholder(event_sequential_id, template_id, party_id)
WITH
input1 AS
(
    SELECT
        event_sequential_id AS i,
        template_id AS t,
        flat_event_witnesses AS ps
    FROM participant_events_consuming_exercise
)
SELECT i, t, p
FROM input1, json_table(ps, '$[*]' columns (p NUMBER PATH '$'));

-- Populate pe_consuming_id_filter_non_stakeholder_informee
INSERT INTO pe_consuming_id_filter_non_stakeholder_informee(event_sequential_id, party_id)
WITH
input1 AS
(
    SELECT
        event_sequential_id AS i,
        etq_array_diff(tree_event_witnesses, flat_event_witnesses) AS ps
    FROM participant_events_consuming_exercise
)
SELECT i, p
FROM input1, json_table(ps, '$[*]' columns (p NUMBER PATH '$'));

-- Populate pe_non_consuming_exercise_filter_nonstakeholder_informees
INSERT INTO pe_non_consuming_id_filter_informee(event_sequential_id, party_id)
WITH
input1 AS
(
    SELECT
        event_sequential_id AS i,
        etq_array_diff(tree_event_witnesses, flat_event_witnesses) AS ps
    FROM participant_events_non_consuming_exercise
)
SELECT i, p
FROM input1, json_table(ps, '$[*]' columns (p NUMBER PATH '$'));

-- Populate participant_transaction_meta
INSERT INTO participant_transaction_meta(transaction_id, event_offset, event_sequential_id_first, event_sequential_id_last)
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
SELECT t, o, first_i, last_i FROM input2, parameters WHERE
   parameters.participant_pruned_up_to_inclusive is null
   or o > parameters.participant_pruned_up_to_inclusive;

DROP FUNCTION etq_array_diff;