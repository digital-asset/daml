ALTER TABLE participant_users
    ADD COLUMN identity_provider_id VARCHAR DEFAULT NULL;

ALTER TABLE participant_party_records
    ADD COLUMN identity_provider_id VARCHAR DEFAULT NULL;
