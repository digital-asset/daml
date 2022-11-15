ALTER TABLE participant_users
    ADD COLUMN identity_provider_id VARCHAR DEFAULT NULL;

ALTER TABLE participant_party_records
    ADD COLUMN identity_provider_id VARCHAR DEFAULT NULL;

CREATE TABLE participant_identity_provider_config
(
    identity_provider_id VARCHAR PRIMARY KEY NOT NULL,
    issuer               VARCHAR             NOT NULL UNIQUE,
    jwks_url             VARCHAR             NOT NULL,
    is_deactivated       BOOLEAN             NOT NULL
);
