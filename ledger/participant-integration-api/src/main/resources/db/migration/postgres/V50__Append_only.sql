-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

/*
------------------------------------------------------------------- OLD

CREATE TABLE public.configuration_entries (
    ledger_offset bytea NOT NULL,
    recorded_at timestamp without time zone NOT NULL,
    submission_id character varying NOT NULL,
    typ character varying NOT NULL,
    configuration bytea NOT NULL,
    rejection_reason character varying,
    CONSTRAINT check_entry CHECK (((((typ)::text = 'accept'::text) AND (rejection_reason IS NULL)) OR (((typ)::text = 'reject'::text) AND (rejection_reason IS NOT NULL))))
);


CREATE TABLE public.package_entries (
    ledger_offset bytea NOT NULL,
    recorded_at timestamp without time zone NOT NULL,
    submission_id character varying,
    typ character varying NOT NULL,
    rejection_reason character varying,
    CONSTRAINT check_package_entry_type CHECK (((((typ)::text = 'accept'::text) AND (rejection_reason IS NULL)) OR (((typ)::text = 'reject'::text) AND (rejection_reason IS NOT NULL))))
);


CREATE TABLE public.packages (
    package_id character varying NOT NULL,
    upload_id character varying NOT NULL,
    source_description character varying,
    size bigint NOT NULL,
    known_since timestamp with time zone NOT NULL,
    ledger_offset bytea NOT NULL,
    package bytea NOT NULL
);



CREATE TABLE public.parameters (
    ledger_id character varying NOT NULL,
    ledger_end bytea,
    external_ledger_end character varying,
    configuration bytea,
    participant_id character varying,
    participant_pruned_up_to_inclusive bytea
);


CREATE TABLE public.participant_command_completions (
    completion_offset bytea NOT NULL,
    record_time timestamp without time zone NOT NULL,
    application_id character varying NOT NULL,
    submitters character varying[] NOT NULL,
    command_id character varying NOT NULL,
    transaction_id character varying,
    status_code integer,
    status_message character varying
);


CREATE TABLE public.participant_command_submissions (
    deduplication_key character varying NOT NULL,
    deduplicate_until timestamp without time zone NOT NULL
);

CREATE TABLE public.participant_contract_witnesses (
    contract_id character varying NOT NULL,
    contract_witness character varying NOT NULL
);

CREATE TABLE public.participant_contracts (
    contract_id character varying NOT NULL,
    template_id character varying NOT NULL,
    create_argument bytea NOT NULL,
    create_stakeholders character varying[],
    create_key_hash bytea,
    create_ledger_effective_time timestamp without time zone
);

CREATE TABLE public.participant_events (
    event_id character varying NOT NULL,
    event_offset bytea NOT NULL,
    contract_id character varying NOT NULL,
    transaction_id character varying NOT NULL,
    ledger_effective_time timestamp without time zone NOT NULL,
    node_index integer NOT NULL,
    command_id character varying,
    workflow_id character varying,
    application_id character varying,
    submitters character varying[],
    create_argument bytea,
    create_signatories character varying[],
    create_observers character varying[],
    create_agreement_text character varying,
    create_consumed_at bytea,
    create_key_value bytea,
    exercise_consuming boolean,
    exercise_choice character varying,
    exercise_argument bytea,
    exercise_result bytea,
    exercise_actors character varying[],
    exercise_child_event_ids character varying[],
    template_id character varying NOT NULL,
    flat_event_witnesses character varying[] DEFAULT '{}'::character varying[] NOT NULL,
    tree_event_witnesses character varying[] DEFAULT '{}'::character varying[] NOT NULL,
    event_sequential_id bigint NOT NULL
);

CREATE SEQUENCE public.participant_events_event_sequential_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE public.parties (
    party character varying NOT NULL,
    display_name character varying,
    explicit boolean NOT NULL,
    ledger_offset bytea,
    is_local boolean NOT NULL
);

CREATE TABLE public.party_entries (
    ledger_offset bytea NOT NULL,
    recorded_at timestamp without time zone NOT NULL,
    submission_id character varying,
    party character varying,
    display_name character varying,
    typ character varying NOT NULL,
    rejection_reason character varying,
    is_local boolean,
    CONSTRAINT check_party_entry_type CHECK (((((typ)::text = 'accept'::text) AND (rejection_reason IS NULL) AND (party IS NOT NULL)) OR (((typ)::text = 'reject'::text) AND (rejection_reason IS NOT NULL))))
);
ALTER TABLE ONLY public.participant_events ALTER COLUMN event_sequential_id SET DEFAULT nextval('public.participant_events_event_sequential_id_seq'::regclass);

ALTER TABLE ONLY public.configuration_entries
    ADD CONSTRAINT configuration_entries_pkey PRIMARY KEY (ledger_offset);

ALTER TABLE ONLY public.flyway_schema_history
    ADD CONSTRAINT flyway_schema_history_pk PRIMARY KEY (installed_rank);

ALTER TABLE ONLY public.package_entries
    ADD CONSTRAINT package_entries_pkey PRIMARY KEY (ledger_offset);

ALTER TABLE ONLY public.packages
    ADD CONSTRAINT packages_pkey PRIMARY KEY (package_id);

ALTER TABLE ONLY public.participant_command_submissions
    ADD CONSTRAINT participant_command_submissions_pkey PRIMARY KEY (deduplication_key);

ALTER TABLE ONLY public.participant_contract_witnesses
    ADD CONSTRAINT participant_contract_witnesses_pkey PRIMARY KEY (contract_id, contract_witness);

ALTER TABLE ONLY public.participant_contracts
    ADD CONSTRAINT participant_contracts_pkey PRIMARY KEY (contract_id);

ALTER TABLE ONLY public.participant_events
    ADD CONSTRAINT participant_events_pkey PRIMARY KEY (event_id);

ALTER TABLE ONLY public.parties
    ADD CONSTRAINT parties_pkey PRIMARY KEY (party);

ALTER TABLE ONLY public.party_entries
    ADD CONSTRAINT party_entries_pkey PRIMARY KEY (ledger_offset);



CREATE INDEX flyway_schema_history_s_idx ON public.flyway_schema_history USING btree (success);
CREATE UNIQUE INDEX idx_configuration_submission ON public.configuration_entries USING btree (submission_id);
CREATE UNIQUE INDEX idx_package_entries ON public.package_entries USING btree (submission_id);
CREATE UNIQUE INDEX idx_party_entries ON public.party_entries USING btree (submission_id);
CREATE INDEX participant_command_completio_completion_offset_application_idx ON public.participant_command_completions USING btree (completion_offset, application_id, submitters);
CREATE UNIQUE INDEX participant_contracts_create_key_hash_idx ON public.participant_contracts USING btree (create_key_hash);
CREATE INDEX participant_events_contract_id_idx ON public.participant_events USING btree (contract_id);
CREATE INDEX participant_events_event_offset ON public.participant_events USING btree (event_offset);
CREATE INDEX participant_events_event_sequential_id ON public.participant_events USING btree (event_sequential_id);
CREATE INDEX participant_events_flat_event_witnesses_idx ON public.participant_events USING gin (flat_event_witnesses);
CREATE INDEX participant_events_template_id_idx ON public.participant_events USING btree (template_id);
CREATE INDEX participant_events_transaction_id_idx ON public.participant_events USING hash (transaction_id);
CREATE INDEX participant_events_tree_event_witnesses_idx ON public.participant_events USING gin (tree_event_witnesses);
ALTER TABLE ONLY public.participant_contract_witnesses
    ADD CONSTRAINT participant_contract_witnesses_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES public.participant_contracts(contract_id);
*/







































------------------------------------------------------------------------------------------------------
-- new



-- drop the mutable tables
DROP table participant_contracts CASCADE;
DROP table participant_contract_witnesses CASCADE;


/*
CREATE TABLE public.parameters (
    ledger_id character varying NOT NULL,
    ledger_end bytea,
    external_ledger_end character varying,
    configuration bytea,
    participant_id character varying,
    participant_pruned_up_to_inclusive bytea
);
*/
DROP TABLE parameters;

CREATE TABLE parameters (
    ledger_id text NOT NULL,
    ledger_end bytea,
    ledger_end_sequential_id bigint, -- new field: the sequential_event_id up to which all events have been ingested
    external_ledger_end text,
    configuration bytea,
    participant_id text,
    participant_pruned_up_to_inclusive bytea
);

/*
CREATE TABLE public.participant_command_completions (
    completion_offset bytea NOT NULL,
    record_time timestamp without time zone NOT NULL,
    application_id character varying NOT NULL,
    submitters character varying[] NOT NULL,
    command_id character varying NOT NULL,
    transaction_id character varying,
    status_code integer,
    status_message character varying
);

and the new is the same hm

CREATE TABLE participant_command_completions (
    completion_offset bytea NOT NULL,
    record_time timestamp without time zone NOT NULL,
    application_id text NOT NULL,
    submitters text[] NOT NULL,
    command_id text NOT NULL,
    transaction_id text,
    status_code integer,
    status_message text
);
*/




/*
CREATE TABLE public.participant_events (
    event_id character varying NOT NULL,
    event_offset bytea NOT NULL,
    contract_id character varying NOT NULL,
    transaction_id character varying NOT NULL,
    ledger_effective_time timestamp without time zone NOT NULL,
    node_index integer NOT NULL,
    command_id character varying,
    workflow_id character varying,
    application_id character varying,
    submitters character varying[],
    create_argument bytea,
    create_signatories character varying[],
    create_observers character varying[],
    create_agreement_text character varying,
    create_consumed_at bytea,
    create_key_value bytea,
    exercise_consuming boolean,
    exercise_choice character varying,
    exercise_argument bytea,
    exercise_result bytea,
    exercise_actors character varying[],
    exercise_child_event_ids character varying[],
    template_id character varying NOT NULL,
    flat_event_witnesses character varying[] DEFAULT '{}'::character varying[] NOT NULL,
    tree_event_witnesses character varying[] DEFAULT '{}'::character varying[] NOT NULL,
    event_sequential_id bigint NOT NULL
);
*/
DROP TABLE participant_events CASCADE;


-- create, divulgence, consuming, and non-consuming events
-- statically partitioned to the individual event types so that the planner has solid statistics
--
-- TODO: reorder small fields to the end to avoid unnecessary padding.
--
CREATE TABLE participant_events (
    ----- MARK removed:
    -- create_consumed_at
    -- exercise_consuming
    ----- MARK added:
    -- event_kind
    -- create_key_hash

    -- * kinds of events
    -- MARK plus
    event_kind smallint NOT NULL, -- Numbers allocated to leave some space for future additions.
                                  -- 0:  divulgence event
                                  -- 10: create event
                                  -- 20: consuming exercise event
                                  -- 25: non-consuming exercise event

    -- * event identification
    event_sequential_id bigserial NOT NULL, -- TODO temporarily readding bigserial for original write paths
    -- NOTE: this must be assigned sequentially by the indexer such that
    -- for all events ev1, ev2 it holds that '(ev1.offset < ev2.offset) <=> (ev1.event_sequential_id < ev2.event_sequential_id)

    event_offset bytea,                                 -- NULL for divulgance events

    -- * transaction metadata
    transaction_id text,                                -- NULL for migrated divulgence events
    ledger_effective_time timestamp without time zone,  -- NULL for migrated divulgence events
    command_id text,
    workflow_id text,
    application_id text,
    submitters text[],

    -- * event metadata
    node_index integer,  -- NULL for migrated divulgence events
    event_id text,       -- NULL for migrated divulgence events
                         -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id text NOT NULL,
    template_id text,     -- TODO @simon@ with the new divulgance model supporting public pkv implementations: we need this to enable NULL-s. Do we need to make involved indexes partial?
    flat_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL,       -- stakeholders of create events and consuming exercise events
    tree_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL,       -- informees for create, exercise, and divulgance events

    -- * divulgence and create events
    create_argument bytea,

    -- * create events only
    create_signatories text[],
    create_observers text[],
    create_agreement_text text,
    create_key_value bytea,
    -- MARK plus
    create_key_hash bytea,   -- This field is newly added; and not present in the fields below. Populate for create events only

    -- * exercise events (consuming and non_consuming)
    exercise_choice text,
    exercise_argument bytea,
    exercise_result bytea,
    exercise_actors text[],
    exercise_child_event_ids text[]



) PARTITION BY LIST (event_kind);


-- Set storage parameters before creating partitions so they follow the pattern.
-- these columns contain data that is generally incompressible, so don't try it
ALTER TABLE participant_events ALTER COLUMN create_key_hash  SET STORAGE EXTERNAL;


CREATE TABLE participant_events_divulgence             PARTITION OF participant_events FOR VALUES IN (0);
CREATE TABLE participant_events_create                 PARTITION OF participant_events FOR VALUES IN (10);
CREATE TABLE participant_events_consuming_exercise     PARTITION OF participant_events FOR VALUES IN (20);
CREATE TABLE participant_events_non_consuming_exercise PARTITION OF participant_events FOR VALUES IN (25);



/* Example create
-[ RECORD 1 ]------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
event_id                 | #0A2438616434626535392D303230382D333433642D623164382D356231366233396237343632:0
event_offset             | \x00000000000000100000000000000000
contract_id              | 006e8c6a0371fff5b543a96d6cef86b2d33198a8fdbe25a48c84f6ff93de9391a4
transaction_id           | 0A2438616434626535392D303230382D333433642D623164382D356231366233396237343632
ledger_effective_time    | 2021-01-26 17:48:09.126317
node_index               | 0
command_id               | ACSagreementText-alpha-6daec7090a03-command-0
workflow_id              |
application_id           | ACSagreementText
submitters               | {ACSagreementText-alpha-6daec7090a03-party-0}
create_argument          | \x0a0136128f010a8c010a4f0a40646338333436303064396365623830643138316630326165346638373762366366383930313361646339373365663930353833393337393833343133363735641204546573741a0544756d6d7912390a086f70657261746f72122d522b41435361677265656d656e74546578742d616c7068612d3664616563373039306130332d70617274792d30
create_signatories       | {ACSagreementText-alpha-6daec7090a03-party-0}
create_observers         | {}
create_agreement_text    | 'ACSagreementText-alpha-6daec7090a03-party-0' operates a dummy.
create_consumed_at       |
create_key_value         |
exercise_consuming       |
exercise_choice          |
exercise_argument        |
exercise_result          |
exercise_actors          |
exercise_child_event_ids |
template_id              | dc834600d9ceb80d181f02ae4f877b6cf89013adc973ef90583937983413675d:Test:Dummy
flat_event_witnesses     | {ACSagreementText-alpha-6daec7090a03-party-0}
tree_event_witnesses     | {ACSagreementText-alpha-6daec7090a03-party-0}
event_sequential_id      | 1
Example non-consuming exercise:
sandbox_db2=# select * from participant_events where not exercise_consuming limit 1;
-[ RECORD 1 ]------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
event_id                 | #0A2461303434393262372D343339382D333532372D613763642D653935633638623133326530:0
event_offset             | \x000000000000002f0000000000000000
contract_id              | 00ea3ff2ffd692462aee11ba3a87da8d362ad2393400f185e0084f12b9fb942237
transaction_id           | 0A2461303434393262372D343339382D333532372D613763642D653935633638623133326530
ledger_effective_time    | 2021-01-26 17:48:11.357721
node_index               | 0
command_id               | ACSnoWitnessedContracts-alpha-6daec7090a03-command-1
workflow_id              |
application_id           | ACSnoWitnessedContracts
submitters               | {ACSnoWitnessedContracts-alpha-6daec7090a03-party-1}
create_argument          |
create_signatories       |
create_observers         |
create_agreement_text    |
create_consumed_at       |
create_key_value         |
exercise_consuming       | f
exercise_choice          | WitnessesCreateNewWitnesses
exercise_argument        | \x0a013612690a670a650a40646338333436303064396365623830643138316630326165346638373762366366383930313361646339373365663930353833393337393833343133363735641204546573741a1b5769746e65737365734372656174654e65775769746e6573736573
exercise_result          | \x0a013612467a440a42303030633335613737396630363231663831303236363562313961353365396336303633373062303538373266333438366534316433386432646161396331303265
exercise_actors          | {ACSnoWitnessedContracts-alpha-6daec7090a03-party-1}
exercise_child_event_ids | {#0A2461303434393262372D343339382D333532372D613763642D653935633638623133326530:1}
template_id              | dc834600d9ceb80d181f02ae4f877b6cf89013adc973ef90583937983413675d:Test:Witnesses
flat_event_witnesses     | {}
tree_event_witnesses     | {ACSnoWitnessedContracts-alpha-6daec7090a03-party-0,ACSnoWitnessedContracts-alpha-6daec7090a03-party-1}
event_sequential_id      | 24
Example concusming exercise:
sandbox_db2=# select * from participant_events where exercise_consuming limit 1;
-[ RECORD 1 ]------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
event_id                 | #0A2439333538356664662D643261332D333961612D393339622D303464643635653533333161:0
event_offset             | \x000000000000002d0000000000000000
contract_id              | 0027cf0df3480ac8cab42a54cb5a2047f3e1c9fe1b4667c1a981ded3fcc3a51cd6
transaction_id           | 0A2439333538356664662D643261332D333961612D393339622D303464643635653533333161
ledger_effective_time    | 2021-01-26 17:48:11.273068
node_index               | 0
command_id               | ACSarchivedContracts-alpha-6daec7090a03-command-3
workflow_id              |
application_id           | ACSarchivedContracts
submitters               | {ACSarchivedContracts-alpha-6daec7090a03-party-0}
create_argument          |
create_signatories       |
create_observers         |
create_agreement_text    |
create_consumed_at       |
create_key_value         |
exercise_consuming       | t
exercise_choice          | DummyChoice1
exercise_argument        | \x0a0136125a0a580a560a40646338333436303064396365623830643138316630326165346638373762366366383930313361646339373365663930353833393337393833343133363735641204546573741a0c44756d6d7943686f69636531
exercise_result          | \x0a013612026200
exercise_actors          | {ACSarchivedContracts-alpha-6daec7090a03-party-0}
exercise_child_event_ids | {}
template_id              | dc834600d9ceb80d181f02ae4f877b6cf89013adc973ef90583937983413675d:Test:Dummy
flat_event_witnesses     | {ACSarchivedContracts-alpha-6daec7090a03-party-0}
tree_event_witnesses     | {ACSarchivedContracts-alpha-6daec7090a03-party-0}
event_sequential_id      | 22
*/



-- Indices
----------

-- completions table
--------------------

CREATE INDEX participant_command_completion_offset_application_idx ON participant_command_completions USING btree (completion_offset, application_id, submitters);


-- events table: shared indices
-------------------------------

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_event_offset ON participant_events USING btree (event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_event_sequential_id ON participant_events USING btree (event_sequential_id);

-- lookup by event-id
CREATE INDEX participant_events_event_id_idx ON participant_events USING btree (event_id);

-- lookup by transaction id
CREATE INDEX participant_events_transaction_id_idx ON participant_events USING btree (transaction_id);

-- filtering by template
CREATE INDEX participant_events_template_id_idx ON participant_events USING btree (template_id);

-- filtering by witnesses (visibility) for some queries used in the implementation of
-- GetActiveContracts (flat), GetTransactions (flat) and GetTransactionTrees.
-- Note that Potsgres has trouble using these indices effectively with our paged access.
-- We might decide to drop them.
CREATE INDEX participant_events_flat_event_witnesses_idx ON participant_events USING gin (flat_event_witnesses);
CREATE INDEX participant_events_tree_event_witnesses_idx ON participant_events USING gin (tree_event_witnesses);


-- specific indices for contract lookups
----------------------------------------

-- lookup by contract id
CREATE INDEX participant_events_create_contract_id_idx ON participant_events_create USING hash (contract_id);
CREATE INDEX participant_events_consuming_exercise_contract_id_idx ON participant_events_consuming_exercise USING hash (contract_id);

-- lookup divulgance events, in order of ingestion
CREATE INDEX participant_events_divulgence_contract_id_idx ON participant_events_divulgence USING btree (contract_id, event_sequential_id);

-- lookup by contract_key
CREATE INDEX participant_events_create_create_key_hash_idx ON participant_events_create USING btree (create_key_hash, event_sequential_id);
