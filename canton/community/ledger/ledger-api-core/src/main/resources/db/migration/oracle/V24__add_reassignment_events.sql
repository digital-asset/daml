---------------------------------------------------------------------------------------------------
-- Events table: Unassign
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_events_unassign (
    -- * fixed-size columns first to avoid padding
    event_sequential_id NUMBER NOT NULL,      -- event identification: same ordering as event_offset

    -- * event identification
    event_offset VARCHAR2(4000) NOT NULL,

    -- * transaction metadata
    update_id VARCHAR2(4000) NOT NULL,
    workflow_id VARCHAR2(4000),

    -- * submitter info (only visible on submitting participant)
    command_id VARCHAR2(4000),

    submitter NUMBER NOT NULL,

    -- * shared event information
    contract_id VARCHAR2(4000) NOT NULL,
    template_id NUMBER NOT NULL,
    flat_event_witnesses CLOB DEFAULT '[]' NOT NULL CONSTRAINT ensure_json_peu_flat_event_witnesses CHECK (flat_event_witnesses IS JSON), -- stakeholders

    -- * common reassignment
    source_domain_id NUMBER NOT NULL,
    target_domain_id NUMBER NOT NULL,
    unassign_id VARCHAR2(4000) NOT NULL,
    reassignment_counter NUMBER NOT NULL,

    -- * unassigned specific
    assignment_exclusivity NUMBER
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
    event_sequential_id NUMBER NOT NULL,      -- event identification: same ordering as event_offset

    -- * event identification
    event_offset VARCHAR2(4000) NOT NULL,

    -- * transaction metadata
    update_id VARCHAR2(4000) NOT NULL,
    workflow_id VARCHAR2(4000),

    -- * submitter info (only visible on submitting participant)
    command_id VARCHAR2(4000),

    submitter NUMBER NOT NULL,

    -- * shared event information
    contract_id VARCHAR2(4000) NOT NULL,
    template_id NUMBER NOT NULL,
    flat_event_witnesses CLOB DEFAULT '[]' NOT NULL CONSTRAINT ensure_json_pea_flat_event_witnesses CHECK (flat_event_witnesses IS JSON), -- stakeholders

    -- * common reassignment
    source_domain_id NUMBER NOT NULL,
    target_domain_id NUMBER NOT NULL,
    unassign_id VARCHAR2(4000) NOT NULL,
    reassignment_counter NUMBER NOT NULL,

    -- * assigned specific
    create_argument BLOB NOT NULL,
    create_signatories CLOB NOT NULL CONSTRAINT ensure_json_assign_signatories CHECK (create_signatories IS JSON),
    create_observers CLOB NOT NULL CONSTRAINT ensure_json_assign_observers CHECK (create_observers IS JSON),
    create_agreement_text VARCHAR2(4000),
    create_key_value BLOB,
    create_key_hash VARCHAR2(4000),
    create_argument_compression SMALLINT,
    create_key_value_compression SMALLINT,
    ledger_effective_time NUMBER NOT NULL,
    driver_metadata BLOB NOT NULL
);

-- sequential_id index for paging
CREATE INDEX participant_events_assign_event_sequential_id ON participant_events_assign (event_sequential_id);

CREATE TABLE pe_unassign_id_filter_stakeholder (
   event_sequential_id NUMBER NOT NULL,
   template_id NUMBER NOT NULL,
   party_id NUMBER NOT NULL
);
CREATE INDEX pe_unassign_id_filter_stakeholder_pts_idx ON pe_unassign_id_filter_stakeholder(party_id, template_id, event_sequential_id);
CREATE INDEX pe_unassign_id_filter_stakeholder_ps_idx  ON pe_unassign_id_filter_stakeholder(party_id, event_sequential_id);
CREATE INDEX pe_unassign_id_filter_stakeholder_s_idx   ON pe_unassign_id_filter_stakeholder(event_sequential_id);

CREATE TABLE pe_assign_id_filter_stakeholder (
   event_sequential_id NUMBER NOT NULL,
   template_id NUMBER NOT NULL,
   party_id NUMBER NOT NULL
);
CREATE INDEX pe_assign_id_filter_stakeholder_pts_idx ON pe_assign_id_filter_stakeholder(party_id, template_id, event_sequential_id);
CREATE INDEX pe_assign_id_filter_stakeholder_ps_idx  ON pe_assign_id_filter_stakeholder(party_id, event_sequential_id);
CREATE INDEX pe_assign_id_filter_stakeholder_s_idx   ON pe_assign_id_filter_stakeholder(event_sequential_id);

