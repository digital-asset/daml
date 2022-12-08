------------------------------------ ETQ Data migration -------------------------------

-- Removes all elements from a that are present in b, essentially computes a - b.
CREATE OR REPLACE FUNCTION etq_array_diff(
    arrayClob1 IN CLOB,
    arrayClob2 IN CLOB
)
RETURN CLOB
IS
     zz CLOB;
BEGIN
    SELECT coalesce(JSON_ARRAYAGG(x), '[]') foo
    INTO zz
    FROM
        (
                SELECT x FROM json_table(arrayClob1, '$[*]' columns (x NUMBER PATH '$'))
        ) xx
    LEFT JOIN
        (
                SELECT y FROM json_table(arrayClob2, '$[*]' columns (y NUMBER PATH '$'))
        ) yy
    ON x = y
    WHERE y IS NULL;
    RETURN zz;
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
SELECT t, o, first_i, last_i FROM input2;

DROP FUNCTION etq_array_diff;