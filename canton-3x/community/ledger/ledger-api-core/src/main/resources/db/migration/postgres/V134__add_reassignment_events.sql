---------------------------------------------------------------------------------------------------
-- Events table: Unassign
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_events_unassign (
    -- * fixed-size columns first to avoid padding
    event_sequential_id BIGINT NOT NULL,      -- event identification: same ordering as event_offset

    -- * event identification
    event_offset TEXT NOT NULL,

    -- * transaction metadata
    update_id TEXT NOT NULL,
    workflow_id TEXT,

    -- * submitter info (only visible on submitting participant)
    command_id TEXT,

    submitter INTEGER NOT NULL,

    -- * shared event information
    contract_id TEXT NOT NULL,
    template_id INTEGER NOT NULL,
    flat_event_witnesses INTEGER[] DEFAULT '{}'::INTEGER[] NOT NULL, -- stakeholders

    -- * common reassignment
    source_domain_id INTEGER NOT NULL,
    target_domain_id INTEGER NOT NULL,
    unassign_id TEXT NOT NULL,
    reassignment_counter BIGINT NOT NULL,

    -- * unassigned specific
    assignment_exclusivity BIGINT
);

-- sequential_id index for paging
CREATE INDEX participant_events_unassign_event_sequential_id ON participant_events_unassign (event_sequential_id);

-- multi-column index supporting per contract per domain lookup before/after sequential id query
CREATE INDEX participant_events_unassign_contract_id_composite ON participant_events_unassign (contract_id, source_domain_id, event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events table: Assign
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_events_assign (
    -- * fixed-size columns first to avoid padding
    event_sequential_id BIGINT NOT NULL,      -- event identification: same ordering as event_offset

    -- * event identification
    event_offset TEXT NOT NULL,

    -- * transaction metadata
    update_id TEXT NOT NULL,
    workflow_id TEXT,

    -- * submitter info (only visible on submitting participant)
    command_id TEXT,

    submitter INTEGER NOT NULL,

    -- * shared event information
    contract_id TEXT NOT NULL,
    template_id INTEGER NOT NULL,
    flat_event_witnesses INTEGER[] DEFAULT '{}'::INTEGER[] NOT NULL, -- stakeholders

    -- * common reassignment
    source_domain_id INTEGER NOT NULL,
    target_domain_id INTEGER NOT NULL,
    unassign_id TEXT NOT NULL,
    reassignment_counter BIGINT NOT NULL,

    -- * assigned specific
    create_argument BYTEA NOT NULL,
    create_signatories INTEGER[] DEFAULT '{}'::INTEGER[] NOT NULL,
    create_observers INTEGER[] DEFAULT '{}'::INTEGER[] NOT NULL,
    create_agreement_text TEXT,
    create_key_value BYTEA,
    create_key_hash TEXT,
    create_argument_compression SMALLINT,
    create_key_value_compression SMALLINT,
    ledger_effective_time BIGINT NOT NULL,
    driver_metadata BYTEA NOT NULL
);

-- sequential_id index for paging
CREATE INDEX participant_events_assign_event_sequential_id ON participant_events_assign (event_sequential_id);

CREATE TABLE pe_unassign_id_filter_stakeholder (
   event_sequential_id BIGINT NOT NULL,
   template_id INTEGER NOT NULL,
   party_id INTEGER NOT NULL
);
CREATE INDEX pe_unassign_id_filter_stakeholder_pts_idx ON pe_unassign_id_filter_stakeholder(party_id, template_id, event_sequential_id);
CREATE INDEX pe_unassign_id_filter_stakeholder_ps_idx  ON pe_unassign_id_filter_stakeholder(party_id, event_sequential_id);
CREATE INDEX pe_unassign_id_filter_stakeholder_s_idx   ON pe_unassign_id_filter_stakeholder(event_sequential_id);

CREATE TABLE pe_assign_id_filter_stakeholder (
   event_sequential_id BIGINT NOT NULL,
   template_id INTEGER NOT NULL,
   party_id INTEGER NOT NULL
);
CREATE INDEX pe_assign_id_filter_stakeholder_pts_idx ON pe_assign_id_filter_stakeholder(party_id, template_id, event_sequential_id);
CREATE INDEX pe_assign_id_filter_stakeholder_ps_idx  ON pe_assign_id_filter_stakeholder(party_id, event_sequential_id);
CREATE INDEX pe_assign_id_filter_stakeholder_s_idx   ON pe_assign_id_filter_stakeholder(event_sequential_id);

