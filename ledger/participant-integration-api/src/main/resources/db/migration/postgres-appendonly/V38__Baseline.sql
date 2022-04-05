-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V38.1: Squash of migrations V1 to V38.
-- This migration was generated using the following procedure:
-- - Apply migrations V1 to V38 to an empty database
-- - Run 'pg_dump -s'
-- - Manually clean up the resulting script
---------------------------------------------------------------------------------------------------

CREATE TABLE configuration_entries (
    ledger_offset bytea NOT NULL,
    recorded_at timestamp without time zone NOT NULL,
    submission_id character varying NOT NULL,
    typ character varying NOT NULL,
    configuration bytea NOT NULL,
    rejection_reason character varying,
    CONSTRAINT check_entry CHECK (((((typ)::text = 'accept'::text) AND (rejection_reason IS NULL)) OR (((typ)::text = 'reject'::text) AND (rejection_reason IS NOT NULL))))
);

CREATE TABLE package_entries (
    ledger_offset bytea NOT NULL,
    recorded_at timestamp without time zone NOT NULL,
    submission_id character varying,
    typ character varying NOT NULL,
    rejection_reason character varying,
    CONSTRAINT check_package_entry_type CHECK (((((typ)::text = 'accept'::text) AND (rejection_reason IS NULL)) OR (((typ)::text = 'reject'::text) AND (rejection_reason IS NOT NULL))))
);

CREATE TABLE packages (
    package_id character varying NOT NULL,
    upload_id character varying NOT NULL,
    source_description character varying,
    size bigint NOT NULL,
    known_since timestamp with time zone NOT NULL,
    ledger_offset bytea NOT NULL,
    package bytea NOT NULL
);

CREATE TABLE parameters (
    ledger_id character varying NOT NULL,
    ledger_end bytea,
    external_ledger_end character varying,
    configuration bytea,
    participant_id character varying
);

CREATE TABLE participant_command_completions (
    completion_offset bytea NOT NULL,
    record_time timestamp without time zone NOT NULL,
    application_id character varying NOT NULL,
    submitting_party character varying NOT NULL,
    command_id character varying NOT NULL,
    transaction_id character varying,
    status_code integer,
    status_message character varying
);

CREATE TABLE participant_command_submissions (
    deduplication_key character varying NOT NULL,
    deduplicate_until timestamp without time zone NOT NULL
);

CREATE TABLE participant_contract_witnesses (
    contract_id character varying NOT NULL,
    contract_witness character varying NOT NULL
);

CREATE TABLE participant_contracts (
    contract_id character varying NOT NULL,
    template_id character varying NOT NULL,
    create_argument bytea NOT NULL,
    create_stakeholders character varying[],
    create_key_hash bytea,
    create_ledger_effective_time timestamp without time zone
);

CREATE TABLE participant_events (
    event_id character varying NOT NULL,
    event_offset bytea NOT NULL,
    contract_id character varying NOT NULL,
    transaction_id character varying NOT NULL,
    ledger_effective_time timestamp without time zone NOT NULL,
    node_index integer NOT NULL,
    command_id character varying,
    workflow_id character varying,
    application_id character varying,
    submitter character varying,
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

CREATE SEQUENCE participant_events_event_sequential_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE participant_events_event_sequential_id_seq OWNED BY participant_events.event_sequential_id;

CREATE TABLE parties (
    party character varying NOT NULL,
    display_name character varying,
    explicit boolean NOT NULL,
    ledger_offset bytea,
    is_local boolean NOT NULL
);

CREATE TABLE party_entries (
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

ALTER TABLE ONLY participant_events ALTER COLUMN event_sequential_id SET DEFAULT nextval('participant_events_event_sequential_id_seq'::regclass);

ALTER TABLE ONLY configuration_entries
    ADD CONSTRAINT configuration_entries_pkey PRIMARY KEY (ledger_offset);

ALTER TABLE ONLY package_entries
    ADD CONSTRAINT package_entries_pkey PRIMARY KEY (ledger_offset);

ALTER TABLE ONLY packages
    ADD CONSTRAINT packages_pkey PRIMARY KEY (package_id);

ALTER TABLE ONLY participant_command_submissions
    ADD CONSTRAINT participant_command_submissions_pkey PRIMARY KEY (deduplication_key);

ALTER TABLE ONLY participant_contract_witnesses
    ADD CONSTRAINT participant_contract_witnesses_pkey PRIMARY KEY (contract_id, contract_witness);

ALTER TABLE ONLY participant_contracts
    ADD CONSTRAINT participant_contracts_pkey PRIMARY KEY (contract_id);

ALTER TABLE ONLY participant_events
    ADD CONSTRAINT participant_events_pkey PRIMARY KEY (event_id);

ALTER TABLE ONLY parties
    ADD CONSTRAINT parties_pkey PRIMARY KEY (party);

ALTER TABLE ONLY party_entries
    ADD CONSTRAINT party_entries_pkey PRIMARY KEY (ledger_offset);

CREATE UNIQUE INDEX idx_configuration_submission ON configuration_entries USING btree (submission_id);

CREATE UNIQUE INDEX idx_package_entries ON package_entries USING btree (submission_id);

CREATE UNIQUE INDEX idx_party_entries ON party_entries USING btree (submission_id);

CREATE INDEX participant_command_completio_completion_offset_application_idx ON participant_command_completions USING btree (completion_offset, application_id, submitting_party);

CREATE UNIQUE INDEX participant_contracts_create_key_hash_idx ON participant_contracts USING btree (create_key_hash);

CREATE INDEX participant_events_contract_id_idx ON participant_events USING btree (contract_id);

CREATE INDEX participant_events_event_offset ON participant_events USING btree (event_offset);

CREATE INDEX participant_events_event_sequential_id ON participant_events USING btree (event_sequential_id);

CREATE INDEX participant_events_flat_event_witnesses_idx ON participant_events USING gin (flat_event_witnesses);

CREATE INDEX participant_events_template_id_idx ON participant_events USING btree (template_id);

CREATE INDEX participant_events_transaction_id_idx ON participant_events USING btree (transaction_id);

CREATE INDEX participant_events_tree_event_witnesses_idx ON participant_events USING gin (tree_event_witnesses);

ALTER TABLE ONLY participant_contract_witnesses
    ADD CONSTRAINT participant_contract_witnesses_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES participant_contracts(contract_id);
