ALTER TABLE participant_users
    ADD identity_provider_id VARCHAR2(255) DEFAULT NULL REFERENCES participant_identity_provider_config (identity_provider_id);

ALTER TABLE participant_party_records
    ADD identity_provider_id VARCHAR2(255) DEFAULT NULL REFERENCES participant_identity_provider_config (identity_provider_id);
