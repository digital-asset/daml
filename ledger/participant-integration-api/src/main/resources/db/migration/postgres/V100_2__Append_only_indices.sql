-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


---------------------------------------------------------------------------------------------------
-- V100.2: Append-only schema
--
-- This step creates indices for the new tables of the append-only schema
---------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------
-- Events table: divulgence
---------------------------------------------------------------------------------------------------

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_divulgence_event_offset ON participant_events_divulgence USING btree (event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_divulgence_event_sequential_id ON participant_events_divulgence USING btree (event_sequential_id);

-- filtering by template
CREATE INDEX participant_events_divulgence_template_id_idx ON participant_events_divulgence USING btree (template_id);

-- filtering by witnesses (visibility) for some queries used in the implementation of
-- GetActiveContracts (flat), GetTransactions (flat) and GetTransactionTrees.
-- Note that Potsgres has trouble using these indices effectively with our paged access.
-- We might decide to drop them.
CREATE INDEX participant_events_divulgence_tree_event_witnesses_idx ON participant_events_divulgence USING gin (tree_event_witnesses);

-- lookup divulgance events, in order of ingestion
CREATE INDEX participant_events_divulgence_contract_id_idx ON participant_events_divulgence USING btree (contract_id, event_sequential_id);


---------------------------------------------------------------------------------------------------
-- Events table: create
---------------------------------------------------------------------------------------------------

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_create_event_offset ON participant_events_create USING btree (event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_create_event_sequential_id ON participant_events_create USING btree (event_sequential_id);

-- lookup by event-id
CREATE INDEX participant_events_create_event_id_idx ON participant_events_create USING btree (event_id);

-- lookup by transaction id
CREATE INDEX participant_events_create_transaction_id_idx ON participant_events_create USING btree (transaction_id);

-- filtering by template
CREATE INDEX participant_events_create_template_id_idx ON participant_events_create USING btree (template_id);

-- filtering by witnesses (visibility) for some queries used in the implementation of
-- GetActiveContracts (flat), GetTransactions (flat) and GetTransactionTrees.
-- Note that Potsgres has trouble using these indices effectively with our paged access.
-- We might decide to drop them.
CREATE INDEX participant_events_create_flat_event_witnesses_idx ON participant_events_create USING gin (flat_event_witnesses);
CREATE INDEX participant_events_create_tree_event_witnesses_idx ON participant_events_create USING gin (tree_event_witnesses);

-- lookup by contract id
CREATE INDEX participant_events_create_contract_id_idx ON participant_events_create USING hash (contract_id);

-- lookup by contract_key
CREATE INDEX participant_events_create_create_key_hash_idx ON participant_events_create USING btree (create_key_hash, event_sequential_id);


---------------------------------------------------------------------------------------------------
-- Events table: consuming exercise
---------------------------------------------------------------------------------------------------

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_consuming_exercise_event_offset ON participant_events_consuming_exercise USING btree (event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_consuming_exercise_event_sequential_id ON participant_events_consuming_exercise USING btree (event_sequential_id);

-- lookup by event-id
CREATE INDEX participant_events_consuming_exercise_event_id_idx ON participant_events_consuming_exercise USING btree (event_id);

-- lookup by transaction id
CREATE INDEX participant_events_consuming_exercise_transaction_id_idx ON participant_events_consuming_exercise USING btree (transaction_id);

-- filtering by template
CREATE INDEX participant_events_consuming_exercise_template_id_idx ON participant_events_consuming_exercise USING btree (template_id);

-- filtering by witnesses (visibility) for some queries used in the implementation of
-- GetActiveContracts (flat), GetTransactions (flat) and GetTransactionTrees.
-- Note that Potsgres has trouble using these indices effectively with our paged access.
-- We might decide to drop them.
CREATE INDEX participant_events_consuming_exercise_flat_event_witnesses_idx ON participant_events_consuming_exercise USING gin (flat_event_witnesses);
CREATE INDEX participant_events_consuming_exercise_tree_event_witnesses_idx ON participant_events_consuming_exercise USING gin (tree_event_witnesses);

-- lookup by contract id
CREATE INDEX participant_events_consuming_exercise_contract_id_idx ON participant_events_consuming_exercise USING hash (contract_id);


---------------------------------------------------------------------------------------------------
-- Events table: non-consuming exercise
---------------------------------------------------------------------------------------------------

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_non_consuming_exercise_event_offset ON participant_events_non_consuming_exercise USING btree (event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_non_consuming_exercise_event_sequential_id ON participant_events_non_consuming_exercise USING btree (event_sequential_id);

-- lookup by event-id
CREATE INDEX participant_events_non_consuming_exercise_event_id_idx ON participant_events_non_consuming_exercise USING btree (event_id);

-- lookup by transaction id
CREATE INDEX participant_events_non_consuming_exercise_transaction_id_idx ON participant_events_non_consuming_exercise USING btree (transaction_id);

-- filtering by template
CREATE INDEX participant_events_non_consuming_exercise_template_id_idx ON participant_events_non_consuming_exercise USING btree (template_id);

-- filtering by witnesses (visibility) for some queries used in the implementation of
-- GetActiveContracts (flat), GetTransactions (flat) and GetTransactionTrees.
-- Note that Potsgres has trouble using these indices effectively with our paged access.
-- We might decide to drop them.
-- NOTE: index name truncated because the full name exceeds the 63 characters length limit
CREATE INDEX participant_events_non_consuming_exercise_flat_event_witnes_idx ON participant_events_non_consuming_exercise USING gin (flat_event_witnesses);
CREATE INDEX participant_events_non_consuming_exercise_tree_event_witnes_idx ON participant_events_non_consuming_exercise USING gin (tree_event_witnesses);


---------------------------------------------------------------------------------------------------
-- Completions table
---------------------------------------------------------------------------------------------------

CREATE INDEX participant_command_completion_offset_application_idx ON participant_command_completions USING btree (completion_offset, application_id);
