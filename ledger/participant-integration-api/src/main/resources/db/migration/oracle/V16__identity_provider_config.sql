CREATE TABLE participant_identity_provider_config
(
    identity_provider_id VARCHAR2(255) PRIMARY KEY NOT NULL,
    issuer               VARCHAR2(4000) NOT NULL UNIQUE,
    jwks_url             VARCHAR2(4000) NOT NULL,
    is_deactivated       NUMBER DEFAULT 0 NOT NULL
);

ALTER TABLE participant_users
    ADD COLUMN identity_provider_id VARCHAR2(255) DEFAULT NULL REFERENCES participant_identity_provider_config (identity_provider_id);

ALTER TABLE participant_party_records
    ADD COLUMN identity_provider_id VARCHAR2(255) DEFAULT NULL REFERENCES participant_identity_provider_config (identity_provider_id);
