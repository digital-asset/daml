CREATE TABLE participant_identity_provider_config
(
    identity_provider_id VARCHAR(255) PRIMARY KEY NOT NULL COLLATE "C",
    issuer               VARCHAR             NOT NULL UNIQUE,
    jwks_url             VARCHAR             NOT NULL,
    is_deactivated       BOOLEAN             NOT NULL
);

ALTER TABLE participant_users
    ADD COLUMN identity_provider_id VARCHAR(255) DEFAULT NULL REFERENCES participant_identity_provider_config (identity_provider_id);

ALTER TABLE participant_party_records
    ADD COLUMN identity_provider_id VARCHAR(255) DEFAULT NULL REFERENCES participant_identity_provider_config (identity_provider_id);
