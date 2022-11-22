
-- Completions
-- TODO ETQ: We had this index on PG but not on Oracle:
-- DROP INDEX participant_command_completion_offset_application_idx;

CREATE INDEX participant_command_completions_application_id_offset_idx ON participant_command_completions(application_id, completion_offset);

-- Flat transactions


CREATE TABLE pe_consuming_exercise_filter_stakeholders (
   event_sequential_id NUMBER NOT NULL,
   template_id NUMBER NOT NULL,
   party_id NUMBER NOT NULL
);
CREATE INDEX pe_consuming_exercise_filter_stakeholders_pts_idx ON pe_consuming_exercise_filter_stakeholders(party_id, template_id, event_sequential_id);
CREATE INDEX pe_consuming_exercise_filter_stakeholders_ps_idx  ON pe_consuming_exercise_filter_stakeholders(party_id, event_sequential_id);
CREATE INDEX pe_consuming_exercise_filter_stakeholders_s_idx   ON pe_consuming_exercise_filter_stakeholders(event_sequential_id);


--- Tree transactions

CREATE TABLE pe_create_filter_nonstakeholder_informees (
   event_sequential_id NUMBER NOT NULL,
   party_id NUMBER NOT NULL
);
CREATE INDEX pe_create_filter_nonstakeholder_informees_ps_idx ON pe_create_filter_nonstakeholder_informees(party_id, event_sequential_id);
CREATE INDEX pe_create_filter_nonstakeholder_informees_s_idx ON pe_create_filter_nonstakeholder_informees(event_sequential_id);


CREATE TABLE pe_consuming_exercise_filter_nonstakeholder_informees (
   event_sequential_id NUMBER NOT NULL,
   party_id NUMBER NOT NULL
);
CREATE INDEX pe_consuming_exercise_filter_nonstakeholder_informees_ps_idx ON pe_consuming_exercise_filter_nonstakeholder_informees(party_id, event_sequential_id);
CREATE INDEX pe_consuming_exercise_filter_nonstakeholder_informees_s_idx ON pe_consuming_exercise_filter_nonstakeholder_informees(event_sequential_id);


CREATE TABLE pe_non_consuming_exercise_filter_informees (
   event_sequential_id NUMBER NOT NULL,
   party_id NUMBER NOT NULL
);
CREATE INDEX pe_non_consuming_exercise_filter_informees_ps_idx ON pe_non_consuming_exercise_filter_informees(party_id, event_sequential_id);
CREATE INDEX pe_non_consuming_exercise_filter_informees_s_idx ON pe_non_consuming_exercise_filter_informees(event_sequential_id);


-- Point-wise lookup

CREATE TABLE participant_transaction_meta(
    transaction_id VARCHAR2(4000) NOT NULL,
    event_offset VARCHAR2(4000) NOT NULL,
    event_sequential_id_from NUMBER NOT NULL,
    event_sequential_id_to NUMBER NOT NULL
);
CREATE INDEX participant_transaction_meta_tid_idx ON participant_transaction_meta(transaction_id);
CREATE INDEX participant_transaction_meta_eventoffset_idx ON participant_transaction_meta(event_offset);

DROP INDEX participant_events_create_transaction_id_idx;
DROP INDEX participant_events_consuming_exercise_transaction_id_idx;
DROP INDEX participant_events_non_consuming_exercise_transaction_id_idx;

--------------------------------------------------------
--------- Data migration -------------------------------
--------------------------------------------------------

-- Removes all elements from a that are present in b, essentially computes a - b.
CREATE OR REPLACE FUNCTION array_diff(
    arrayClob1 IN CLOB,
    arrayClob2 IN CLOB
)
RETURN CLOB
IS
        arrayJson1 json_array_t := json_array_t.parse(arrayClob1);
        outputJsonArray json_array_t := json_array_t ('[]');
        filterExpression varchar2(100);
BEGIN
    FOR i IN 0 .. arrayJson1.get_size - 1
    LOOP
        -- `$[*]` selects each element of the array
        -- `(@ == v)` is a filter expression that check whether each matched element is equal to some value `v`
        filterExpression := '$[*]?(@ == ' || (arrayJson1.get(i).to_clob()) ||')';
        IF NOT json_exists(arrayClob2, filterExpression)
        THEN
            outputJsonArray.append(arrayJson1.get(i));
        END IF;
    END LOOP;
    RETURN outputJsonArray.to_clob();
END;
/

---- Populate pe_create_filter_nonstakeholder_informees
INSERT INTO pe_create_filter_nonstakeholder_informees(event_sequential_id, party_id)
WITH
input1 AS
(
    SELECT
        event_sequential_id AS i,
        array_diff(tree_event_witnesses, flat_event_witnesses) AS ps
    FROM participant_events_create
)
SELECT i, p
FROM input1, json_table(ps, '$[*]' columns (p NUMBER PATH '$'));

--DROP TABLE pe_create_filter_nonstakeholder_informees2;
--CREATE TABLE pe_create_filter_nonstakeholder_informees2 (
--   event_sequential_id NUMBER NOT NULL,
--   party_id NUMBER NOT NULL
--);
--WITH
--t1 AS (
--SELECT row_number() over (PARTITION BY event_sequential_id, party_id ORDER BY event_sequential_id) rn, t1.* from pe_create_filter_nonstakeholder_informees t1
--),
--t2 AS (
--SELECT row_number() over (PARTITION BY event_sequential_id, party_id ORDER BY event_sequential_id) rn, t2.* from pe_create_filter_nonstakeholder_informees2 t2
--),
--unmatched AS (
--	SELECT t1.event_sequential_id, t2.event_sequential_id
--	FROM t1
--	FULL JOIN t2
--	ON t1.event_sequential_id = t2.event_sequential_id
--	AND t1.party_id = t2.party_id
--	AND t1.rn = t2.rn
--	WHERE t1.event_sequential_id IS NULL OR t2.event_sequential_id IS NULL
--)
--SELECT count(1) FROM unmatched;


---- Populate pe_consuming_exercise_filter_stakeholders
INSERT INTO pe_consuming_exercise_filter_stakeholders(event_sequential_id, template_id, party_id)
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
--DROP TABLE pe_consuming_exercise_filter_stakeholders2;
--CREATE TABLE pe_consuming_exercise_filter_stakeholders2 (
--   event_sequential_id NUMBER NOT NULL,
--   template_id NUMBER NOT NULL,
--   party_id NUMBER NOT NULL
--);
--WITH
--t1 AS (
--SELECT row_number() over (PARTITION BY event_sequential_id, party_id, template_id ORDER BY event_sequential_id) rn, t1.* from pe_consuming_exercise_filter_stakeholders t1
--),
--t2 AS (
--SELECT row_number() over (PARTITION BY event_sequential_id, party_id, template_id ORDER BY event_sequential_id) rn, t2.* from pe_consuming_exercise_filter_stakeholders2 t2
--),
--unmatched AS (
--	SELECT t1.event_sequential_id, t2.event_sequential_id
--	FROM t1
--	FULL JOIN t2
--	ON t1.event_sequential_id = t2.event_sequential_id
--	AND t1.party_id = t2.party_id
--	AND t1.template_id = t2.template_id
--	AND t1.rn = t2.rn
--	WHERE t1.event_sequential_id IS NULL OR t2.event_sequential_id IS NULL
--)
--SELECT count(1) FROM unmatched;


---- Populate pe_consuming_exercise_filter_nonstakeholder_informees
INSERT INTO pe_consuming_exercise_filter_nonstakeholder_informees(event_sequential_id, party_id)
WITH
input1 AS
(
    SELECT
        event_sequential_id AS i,
        array_diff(tree_event_witnesses, flat_event_witnesses) AS ps
    FROM participant_events_consuming_exercise
)
SELECT i, p
FROM input1, json_table(ps, '$[*]' columns (p NUMBER PATH '$'));

--DROP TABLE pe_consuming_exercise_filter_nonstakeholder_informees2;
--CREATE TABLE pe_consuming_exercise_filter_nonstakeholder_informees2 (
--   event_sequential_id NUMBER NOT NULL,
--   party_id NUMBER NOT NULL
--);
--WITH
--t1 AS (
--SELECT row_number() over (PARTITION BY event_sequential_id, party_id ORDER BY event_sequential_id) rn, t1.* from pe_consuming_exercise_filter_nonstakeholder_informees t1
--),
--t2 AS (
--SELECT row_number() over (PARTITION BY event_sequential_id, party_id ORDER BY event_sequential_id) rn, t2.* from pe_consuming_exercise_filter_nonstakeholder_informees2 t2
--),
--unmatched AS (
--	SELECT t1.event_sequential_id, t2.event_sequential_id
--	FROM t1
--	FULL JOIN t2
--	ON t1.event_sequential_id = t2.event_sequential_id
--	AND t1.party_id = t2.party_id
--	AND t1.rn = t2.rn
--	WHERE t1.event_sequential_id IS NULL OR t2.event_sequential_id IS NULL
--)
--SELECT count(1) FROM unmatched;

---- Populate pe_non_consuming_exercise_filter_nonstakeholder_informees
INSERT INTO pe_non_consuming_exercise_filter_informees(event_sequential_id, party_id)
WITH
input1 AS
(
    SELECT
        event_sequential_id AS i,
        array_diff(tree_event_witnesses, flat_event_witnesses) AS ps
    FROM participant_events_non_consuming_exercise
)
SELECT i, p
FROM input1, json_table(ps, '$[*]' columns (p NUMBER PATH '$'));

--DROP TABLE pe_non_consuming_exercise_filter_informees2;
--CREATE TABLE pe_non_consuming_exercise_filter_informees2 (
--   event_sequential_id NUMBER NOT NULL,
--   party_id NUMBER NOT NULL
--);
--WITH
--t1 AS (
--SELECT row_number() over (PARTITION BY event_sequential_id, party_id ORDER BY event_sequential_id) rn, t1.* from pe_non_consuming_exercise_filter_informees t1
--),
--t2 AS (
--SELECT row_number() over (PARTITION BY event_sequential_id, party_id ORDER BY event_sequential_id) rn, t2.* from pe_non_consuming_exercise_filter_informees2 t2
--),
--unmatched AS (
--	SELECT t1.event_sequential_id, t2.event_sequential_id
--	FROM t1
--	FULL JOIN t2
--	ON t1.event_sequential_id = t2.event_sequential_id
--	AND t1.party_id = t2.party_id
--	AND t1.rn = t2.rn
--	WHERE t1.event_sequential_id IS NULL OR t2.event_sequential_id IS NULL
--)
--SELECT count(1) FROM unmatched;

---- Populate participant_transaction_meta
INSERT INTO participant_transaction_meta(transaction_id, event_offset, event_sequential_id_from, event_sequential_id_to)
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

--DROP TABLE participant_transaction_meta2;
--CREATE TABLE participant_transaction_meta2(
--    transaction_id VARCHAR2(4000) NOT NULL,
--    event_offset VARCHAR2(4000) NOT NULL,
--    event_sequential_id_from NUMBER NOT NULL,
--    event_sequential_id_to NUMBER NOT NULL
--);
--WITH
--t1 AS (
--SELECT row_number() over (PARTITION BY transaction_id, event_offset, event_sequential_id_from, event_sequential_id_to ORDER BY event_offset) rn, t1.* from participant_transaction_meta t1
--),
--t2 AS (
--SELECT row_number() over (PARTITION BY transaction_id, event_offset, event_sequential_id_from, event_sequential_id_to ORDER BY event_offset) rn, t2.* from participant_transaction_meta2 t2
--),
--unmatched AS (
--	SELECT t1.event_offset, t2.event_offset
--	FROM t1
--	FULL JOIN t2
--	ON t1.transaction_id = t2.transaction_id
--	AND t1.event_offset = t2.event_offset
--	AND t1.event_sequential_id_from = t2.event_sequential_id_from
--	AND t1.event_sequential_id_to = t2.event_sequential_id_to
--	AND t1.rn = t2.rn
--	WHERE t1.event_offset IS NULL OR t2.event_offset IS NULL
--)
--SELECT count(1) FROM unmatched;