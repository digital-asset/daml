
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
   event_sequential_id NUMBER NOT NULL,
   template_id NUMBER NOT NULL,
   party_id NUMBER NOT NULL
);
CREATE INDEX pe_consuming_id_filter_stakeholder_pts_idx ON pe_consuming_id_filter_stakeholder(party_id, template_id, event_sequential_id);
CREATE INDEX pe_consuming_id_filter_stakeholder_ps_idx  ON pe_consuming_id_filter_stakeholder(party_id, event_sequential_id);
CREATE INDEX pe_consuming_id_filter_stakeholder_s_idx   ON pe_consuming_id_filter_stakeholder(event_sequential_id);


--- Tree transactions

CREATE TABLE pe_create_id_filter_non_stakeholder_informee (
   event_sequential_id NUMBER NOT NULL,
   party_id NUMBER NOT NULL
);
CREATE INDEX pe_create_id_filter_non_stakeholder_informee_ps_idx ON pe_create_id_filter_non_stakeholder_informee(party_id, event_sequential_id);
CREATE INDEX pe_create_id_filter_non_stakeholder_informee_s_idx ON pe_create_id_filter_non_stakeholder_informee(event_sequential_id);


CREATE TABLE pe_consuming_id_filter_non_stakeholder_informee (
   event_sequential_id NUMBER NOT NULL,
   party_id NUMBER NOT NULL
);
CREATE INDEX pe_consuming_id_filter_non_stakeholder_informee_ps_idx ON pe_consuming_id_filter_non_stakeholder_informee(party_id, event_sequential_id);
CREATE INDEX pe_consuming_id_filter_non_stakeholder_informee_s_idx ON pe_consuming_id_filter_non_stakeholder_informee(event_sequential_id);


CREATE TABLE pe_non_consuming_id_filter_informee (
   event_sequential_id NUMBER NOT NULL,
   party_id NUMBER NOT NULL
);
CREATE INDEX pe_non_consuming_id_filter_informee_ps_idx ON pe_non_consuming_id_filter_informee(party_id, event_sequential_id);
CREATE INDEX pe_non_consuming_id_filter_informee_s_idx ON pe_non_consuming_id_filter_informee(event_sequential_id);


-- Point-wise lookup

CREATE TABLE participant_transaction_meta(
    transaction_id VARCHAR2(4000) NOT NULL,
    event_offset VARCHAR2(4000) NOT NULL,
    event_sequential_id_first NUMBER NOT NULL,
    event_sequential_id_last NUMBER NOT NULL
);
CREATE INDEX participant_transaction_meta_tid_idx ON participant_transaction_meta(transaction_id);
CREATE INDEX participant_transaction_meta_event_offset_idx ON participant_transaction_meta(event_offset);
